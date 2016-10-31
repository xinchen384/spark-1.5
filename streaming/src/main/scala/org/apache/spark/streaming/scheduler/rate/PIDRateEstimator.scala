/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.scheduler.rate

import org.apache.spark.Logging
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.Queue;

import util.control.Breaks._
/**
 * Implements a proportional-integral-derivative (PID) controller which acts on
 * the speed of ingestion of elements into Spark Streaming. A PID controller works
 * by calculating an '''error''' between a measured output and a desired value. In the
 * case of Spark Streaming the error is the difference between the measured processing
 * rate (number of elements/processing delay) and the previous rate.
 *
 * @see https://en.wikipedia.org/wiki/PID_controller
 *
 * @param batchIntervalMillis the batch duration, in milliseconds
 * @param proportional how much the correction should depend on the current
 *        error. This term usually provides the bulk of correction and should be positive or zero.
 *        A value too large would make the controller overshoot the setpoint, while a small value
 *        would make the controller too insensitive. The default value is 1.
 * @param integral how much the correction should depend on the accumulation
 *        of past errors. This value should be positive or 0. This term accelerates the movement
 *        towards the desired value, but a large value may lead to overshooting. The default value
 *        is 0.2.
 * @param derivative how much the correction should depend on a prediction
 *        of future errors, based on current rate of change. This value should be positive or 0.
 *        This term is not used very often, as it impacts stability of the system. The default
 *        value is 0.
 * @param minRate what is the minimum rate that can be estimated.
 *        This must be greater than zero, so that the system always receives some data for rate
 *        estimation to work.
 */
private[streaming] class PIDRateEstimator(
    batchIntervalMillis: Long,
    proportional: Double,
    integral: Double,
    derivative: Double,
    minRate: Double
  ) extends RateEstimator with Logging {

  private var firstRun: Boolean = true
  private var latestTime: Long = -1L
  private var latestRate: Double = -1D
  private var latestError: Double = -1L
  //xin
  private var myLatestRate: Double = -1D
  private var cpLen = -1 
  private var cpOffset = -1 
  private var cpLenFixed: Boolean = false 
  private var startPIDCollect: Boolean = false 
  private var startPIDPredict: Boolean = false 

  private var checkpointId: Double = -1L 
  private var jobNumRecords: Double = -1L 
  private var lastCpRate: Double = -1L 
  private var myMinRate: Double = -1L 
  private var receiverNum: Double = -1L 
  private var lastRNum: Double = -1L 
  private val rNum:Double = 6
  private var delayIntegral:Double = 0.2

  require(
    batchIntervalMillis > 0,
    s"Specified batch interval $batchIntervalMillis in PIDRateEstimator is invalid.")
  require(
    proportional >= 0,
    s"Proportional term $proportional in PIDRateEstimator should be >= 0.")
  require(
    integral >= 0,
    s"Integral term $integral in PIDRateEstimator should be >= 0.")
  require(
    derivative >= 0,
    s"Derivative term $derivative in PIDRateEstimator should be >= 0.")
  require(
    minRate > 0,
    s"Minimum rate in PIDRateEstimator should be > 0")

  logInfo(s"Created PIDRateEstimator with proportional = $proportional, integral = $integral, " +
    s"derivative = $derivative, min rate = $minRate")
  def compute(
      time: Long, // in milliseconds
      numElements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long, // in milliseconds
      num: Int,
      totalEle: Long,
      Id: Long
    ): Option[(Long, Double, Long, Int)] = {
      checkpointId = Id
      if ( cpLenFixed == false && cpLen > 0 && checkpointId < cpLen - 1 ){ 
        cpLenFixed = true 
        cpOffset = cpLen / 2
      }
      else if ( cpLenFixed == false && checkpointId > cpLen - 1 ) 
        cpLen = checkpointId.toInt + 1
      receiverNum = num
      jobNumRecords = totalEle
      compute(time, numElements, processingDelay, schedulingDelay)
  }

  def linearRegression(xlist: Seq[Double], ylist: Seq[Double]): (Double, Double)={
    val xbar = xlist.sum / xlist.length
    val ybar = ylist.sum / ylist.length
    val xx_bar = xlist.map( x => (x-xbar)*(x-xbar) ).sum
    val xy_bar = (xlist zip ylist).map{case (x,y) => (x-xbar)*(y-ybar) }.sum
    val beta1 = xy_bar/xx_bar
    return (beta1, ybar-beta1*xbar)
  }

  // the arrays start with the checkpointing job on 0
  // the first element corresponds to the checkpointId 5 
  val numTupleQueue = new Queue[Double]
  val processTimeQueue = new Queue[Double]
  val numCPQueue = new Queue[Double]
  val timeCPQueue = new Queue[Double]
  val delayTimeQueue = new Queue[Double]

  val stableDelay = new ListBuffer[Double]
  val lastDelay = new ListBuffer[Double]

  val actualDelay = new ListBuffer[Double]
  val expectedDelay = new ListBuffer[Double]
  val actualTime = new ListBuffer[Double] 
  val expectedTime = new ListBuffer[Double]
  val numTuplesJobs = new ListBuffer[Double]
  // the number of actual elements collected
  val actualNum = new ListBuffer[Double]
  val noStartDelay = new ListBuffer[Double]

  val historySize = 30
  val sampleProportion = 0.5
  // do the prediction of the delay and the reduction every 3 circles
  val delayPredictInterval = 1 
  var emptyJobTime:Double = 600
  val smallJobTime = new Queue[Double]
  val cpDelayTime = new Queue[Double]
  val smallLen = 10
  var currentRegionDelay: Double = 0
  
  // the number of "NUM" for the following jobs 
  var numLen = 0 
  // the last num set by the job, used to check the actual num
  var lastSetNum: Double = -1
  var lowJobNum: Double = 0
  var initCount: Double = 0
  val minNum: Double = 5
  var avgTime: Double = 1000 
  var avgRate:Double = -1
  var startDelay: Double = 0
  var goodNum: Double = 0

  var lastEmpty = false
  var lastJobId: Double = -1D
  val mQueue = new Queue[Double]
  //if true, it represents the decreasing, going to reduce the delay with NUM
  //otherwise, the delay is high or increasing, going to adjust the rate 
  var delayTrend: Boolean = true 

  def listMean(list: ListBuffer[Double]): Double = {
    list.reduceLeft(_ + _) / list.size.toDouble
  }
  def listStd(list: ListBuffer[Double]): Double = {
    //val data = list.sorted
    var sum: Double = 0.0
    val mean = listMean(list)
    for (ele <- list){
      sum += math.pow( (ele-mean), 2 )
    }
    math.sqrt(sum/list.size.toDouble)
  }

  def updateActualHistory(time: Double, delay: Double, id: Double, num: Double){
      val myId = (id.toInt - cpOffset + cpLen)%cpLen
      actualTime(myId) += time
      actualDelay(myId) += delay
      actualNum(myId) += 1
      if (myId != 0 && num == minNum*rNum && time < batchIntervalMillis){
        smallJobTime.enqueue(time)
        if (smallJobTime.length > smallLen) 
          smallJobTime.dequeue()
      } 
      if (myId == 0 && num == minNum*rNum){
        cpDelayTime.enqueue(time-batchIntervalMillis)
        if (cpDelayTime.length > smallLen) 
          cpDelayTime.dequeue()
      }
  }
  def getSmallJobTime(): Double = {
    xinLogInfo(s"xin PID controller smallJobTime Array: $smallJobTime")
    if (smallJobTime.length <= 2){
      emptyJobTime 
    }
    else {
      (smallJobTime.sum - smallJobTime.max - smallJobTime.min)/(smallJobTime.length - 2)
    }
  }
  def getCpDelayTime(): Double = {
    xinLogInfo(s"xin PID controller CPdelay Array: $cpDelayTime")
    if (cpDelayTime.length <= 2){
      batchIntervalMillis - emptyJobTime 
    }
    else {
      (cpDelayTime.sum - cpDelayTime.max - cpDelayTime.min)/(cpDelayTime.length - 2)
    }
  }
  def getFullJobNum(): Double = {
    if (goodNum == 0)
        goodNum = getNewNumber(numTupleQueue, processTimeQueue, batchIntervalMillis, 0, 0)
    goodNum
  }
  def clearActual() = {
    for (i <- 1 to cpLen)
        actualTime(i-1) = 0
    for (i <- 1 to cpLen)
        actualDelay(i-1) = 0
    for (i <- 1 to cpLen)
        actualNum(i-1) = 0
    for (i <- 1 to cpLen)
        noStartDelay(i-1) = 0
  }
  def updatePIDHistory(num: Double, time: Double, delay: Double, id: Double){
    // start to collect the delay and processing time
    // at the first round directly set num to 0 
    
    // enqueue elements and dequeue for a good size
    if (id == cpOffset){
      numCPQueue.enqueue(num)
      timeCPQueue.enqueue(time)
    }else{
      numTupleQueue.enqueue(num)
      processTimeQueue.enqueue(time)
    }
    delayTimeQueue.enqueue(delay) 
    if ( delayTimeQueue.length > historySize ){
      delayTimeQueue.dequeue()
    }
    if (id == cpOffset && numCPQueue.length > historySize){
      numCPQueue.dequeue()
      timeCPQueue.dequeue()
    } 
    if (id != cpOffset && numTupleQueue.length > historySize){
      numTupleQueue.dequeue()
      processTimeQueue.dequeue()
    }

  }

  def initializeList(){
    if (stableDelay.length == 0){
      for (i <- 1 to cpLen)
        stableDelay += 0
    }
    if (lastDelay.length == 0)
      for (i <- 1 to cpLen)
        lastDelay += 0

    if (actualNum.length == 0)
      for (i <- 1 to cpLen)
        actualNum += 0
    if (actualDelay.length == 0)
      for (i <- 1 to cpLen)
        actualDelay += 0
    if (expectedDelay.length == 0)
      for (i <- 1 to cpLen)
        expectedDelay += 0
    if (actualTime.length == 0)
      for (i <- 1 to cpLen)
        actualTime += 0
    if (expectedTime.length == 0)
      for (i <- 1 to cpLen)
        expectedTime += batchIntervalMillis 
    if (noStartDelay.length == 0)
      for (i <- 1 to cpLen)
        noStartDelay += 0

    if (numTuplesJobs.length == 0)
      for (i <- 1 to cpLen)
        numTuplesJobs += -1
  }

  // used to detect the pattern of the job
  def getStableDelay(tempDelay: ListBuffer[Double]): Double = {
    val meanDelay = listMean(tempDelay)
    val stdDelay = listStd(tempDelay) 
    val minDelay = tempDelay.min
    // to be conservative
    return minDelay
    if (stdDelay <= meanDelay * 0.1){
      return meanDelay.max(minDelay)
    }else{
      return minDelay 
    }
  }

  // used to detect the trend 
  // if it rises, need to lower the rate
  // it it decreases, need to raise the rate
  def getDelayTrend(tempDelay: ListBuffer[Double]): (Double, Double) = {
    var xlist = new ListBuffer[Double]
    for (i <- 1 to tempDelay.length){
      xlist += i 
    }
    //println(s" trend xarray $xlist")
    //println(s" trend yarray $tempDelay")
    val (beta1, beta0) = linearRegression(xlist, tempDelay)
    val lowDelay = beta1 + beta0
    val highDelay = beta1 * tempDelay.length + beta0
    return (lowDelay, (highDelay-lowDelay))
  }

  def calculateMinNum(id: Double, ptime: Double, delay: Double) = {
    val currentId = id.toInt
    var previousNum = numTuplesJobs(currentId)
    if (currentId == 0){
      numTuplesJobs(currentId) = minNum
      if (previousNum != minNum) 
        lowJobNum = 1
    }
    val pDelay = noStartDelay(currentId) + delay 
    //we do not consider the start delay of the next job 
    //this delay time only needs to be reduced once for the region
    //val nextJobDelay = delay + ptime - batchIntervalMillis 
    val selfTime = batchIntervalMillis - pDelay 
    val nDelay = noStartDelay(currentId+1) 
    val leftTime = selfTime - nDelay
    if (pDelay + nDelay > batchIntervalMillis*0.1){
      if (leftTime >= emptyJobTime) {
        expectedTime(currentId) = leftTime
      } else if (selfTime > emptyJobTime){
      } else {
        numTuplesJobs(currentId) = minNum
        if (leftTime > 0)
          expectedTime(currentId) = leftTime 
        else 
          expectedTime(currentId) = 0 
        if (previousNum != minNum){ 
          lowJobNum = currentId + 1 
        } 
      }

    }
  }
  // the id starts from cp job at index 0
  def handleJobDelay(id: Double): Boolean = {
    val currentId = id.toInt
    val lastId = (currentId - 1 + cpLen)%cpLen

    if (numTuplesJobs(lastId) != minNum){
      if (lastId == 0){
        expectedDelay(0) = 0
        numTuplesJobs(0) = minNum 
        lowJobNum = 1
        // useless
        expectedTime(0) = batchIntervalMillis + emptyJobTime
      }
      // lets try this first
      return true
    }
     
    // num tuple of last index is 0
    // input is: (expectedDelay, Time, Num) and (the actualDelay, Time)
    var avgDelay:Double = 0
    var estimatedNum:Double = 0
    var previousNum = numTuplesJobs(currentId)
    if ( actualDelay(currentId) > batchIntervalMillis*0.1 ){
      //if (expectedDelay(currentId) > 0){
      //  avgDelay = (expectedDelay(currentId)+actualDelay(currentId))/2
      //}else
      //  avgDelay = actualDelay(currentId) 
      //expectedDelay(currentId) = actualDelay(currentId)
      //avgDelay = actualDelay(currentId) 
      avgDelay = noStartDelay(currentId) 
    }

    val leftTime = batchIntervalMillis - avgDelay 
    if ( leftTime <= emptyJobTime ){
      if (leftTime > 0)
        expectedTime(currentId) = leftTime  
      else 
        // the delay is too much
        expectedTime(currentId) = 0  
      estimatedNum = minNum 
      numTuplesJobs(currentId) = minNum 
      lowJobNum = currentId + 1
    }
    else {
      expectedTime(currentId) = leftTime  
      estimatedNum = getJobNum(currentId)
      numTuplesJobs(currentId) = estimatedNum
      lowJobNum = currentId 
    } 
    var res = false 
    if (estimatedNum != previousNum){
      // two cases here: one is that this is the first setting
      // the other is the updated case
      // if it's num to zero, this is good to try out
      // it it's num to num, this is the last step
      // if it's zero to num, reset the following to normal 
      res = true
    }
    if ( previousNum == minNum && estimatedNum > minNum ){
      for (i <- (currentId+1) to cpLen-1 ){
        expectedTime(i) = batchIntervalMillis
        expectedDelay(i) = 0
        numTuplesJobs(i) = -1
      } 
    }
    
    // after setting with 0 num, not sure about the processing time of the empty job
    // might including the cp job
    // for the other jobs, the calculation based on error should be more accurate
    //if ( lastId == 0 || expectedTime(lastId) == -1 )

    // since the num of the previous job is 0, the delay would not change 
    //expectedDelay(currentId) = actualDelay(currentId)
    // actually, the expected and the actual delay should be the same here
    // check it ???
    //expectedDelay(currentId) = actualDelay(lastId) + actualTime(lastId) - batchIntervalMillis 

    // the delay would not change
    return res 
  }
  
  def delayReduction() = {
    for (i <- 1 to cpLen){
      //val delay = (actualDelay(i-1) - startDelay)
      val delay = actualDelay(i-1) 
      if (delay >= 0)
      noStartDelay(i-1) = delay 
      else
      noStartDelay(i-1) = 0 
    }
    xinLogInfo(s"xin PPIDRateEstimator Before expectedTime $expectedTime expectedDelay: $expectedDelay actualTime $actualTime actualDelay: $actualDelay startDelay $startDelay noStartDelayArray $noStartDelay numTuplesJObs $numTuplesJobs")
    var id = 1 
    while (  handleJobDelay(id) == false ){
      id += 1
    } 
   xinLogInfo(s"xin PPIDRateEstimator after expectedTime $expectedTime expectedDelay: $expectedDelay actualTime $actualTime actualDelay: $actualDelay numTuplesJObs $numTuplesJobs")
    
  }
  // when the average delay is large enough  
  // update the expected processing time
  // 1. only reduce the time of the first job
  // 2. remember the zero input of the first jobs
  // 3. how to minimal time for a job
  def loadErrorStableDelay() = {
    if ( historySize % cpLen != 0 ){
      //return null
    }
    val mylen = historySize / cpLen
    var tempDelay = new ListBuffer[Double]
    for (i <- 1 to mylen)
      tempDelay += 0

    breakable {
    for (i <- 0 to cpLen-1){
      //for (j <- 0 to mylen-1)
      //  tempDelay(j) = delayTimeQueue(i+j*cpLen)
      val lastId = (i + cpLen - 1)%cpLen
      if ( getStableDelay(tempDelay) >= 0.1*batchIntervalMillis ){
        //stableDelay only stores the delay time in current circle
        stableDelay(i) = getStableDelay(tempDelay) 
        if (numTuplesJobs(lastId) != 0){
          expectedTime(lastId) = expectedTime(lastId) - stableDelay(i) 
          val cpId = (lastId + cpOffset + cpLen)%cpLen
          val jobNum = getJobNum(cpId)
          if ( jobNum < 0 ) numTuplesJobs(lastId) = 0
          else numTuplesJobs(lastId) = jobNum 

          val currentDelay = stableDelay(i)
          val etime = expectedTime(lastId) 
          val numJob = numTuplesJobs(lastId)
          xinLogInfo(s"xin PPIDRateEstimator Important update job $lastId reduce $currentDelay ($tempDelay) to $etime with $numJob")
          break
        }
      }
      //previousDelay = stableDelay(i)
    }
    }
  }
  def getDelayList(): ListBuffer[Double] = {
    //initializeList()
    //myDelay.clear()
    var myDelay = new ListBuffer[Double]
    for (i <- 1 to cpLen){
      if ( i-1 >= lowJobNum ){
        myDelay += actualDelay(i-1)
      }
    }
    myDelay
  }
  // assume the historical delay time is already 
  // updated into delay TimeQueue
  def loadDelay() = {
    if ( historySize % cpLen != 0 ){
      //return null
    }
    //initializeList()
    for (i <- 1 to cpLen){
      if ( i-1 < lowJobNum ){
        lastDelay(i-1) = 0
      } else {
        lastDelay(i-1) = actualDelay(i-1)
      } 
    }
  }
  
  def desiredLine(xlist: Seq[Double], ylist: Seq[Double], mytime: Double): (Double, Double)={
    val oneDuration = 100
    var filteredX = new ListBuffer[Double]
    var filteredY = new ListBuffer[Double]
    //val midTime = mytime - oneDuration
    val midTime = mytime 
    for (i <- 0 to ylist.length-1){
      if (ylist(i) >= midTime-oneDuration && ylist(i) <= midTime+oneDuration){
        filteredX += xlist(i)
        filteredY += ylist(i)
      }
    }
    //xinLogInfo(s"xin PPIDRateEstimator desiredLine filteredX $filteredX filteredY $filteredY")

    //if ( filteredX.length == 0 )
    //  return (0, 0) 
    //if ( filteredX.length < sampleProportion * xlist.length ){
    if ( filteredX.length <= 1 ){
      linearRegression(xlist, ylist)
    }
    else{
      linearRegression(filteredX, filteredY)  
    }
  }

  // used to set the num of job for a specific job
  def getJobNum(cpId: Int): Double = {
    // every cpLen interval, the expectedTime should be updated 
    // by calling loadDelay()
    val id = (cpId)%cpLen
    if (cpId < 0 || cpId >= cpLen) return -1 
    val ytime = expectedTime(id)
    if ( ytime < emptyJobTime ) return minNum 
    if (id == 0){
      getNewNumber( numCPQueue, timeCPQueue, ytime, 0, 0 )
    } else{
      if (goodNum == 0)
        goodNum = getNewNumber(numTupleQueue, processTimeQueue, batchIntervalMillis, 0, 0)
      if ( ytime < batchIntervalMillis ){
        ytime/batchIntervalMillis * goodNum
      } else{
        goodNum
      }
    }
    //getNewNumber( numTupleQueue, processTimeQueue, ytime, 0, 0 ) 
  }
  // used to predict the new rate(Num of tuples)
  // xlist, ylist are the historical records for (num, processTime)
  // mytime is the expected processing time (1000ms)
  // xNum and yTime is the actual ( num of tuples, the processTim )
  // when xNum and yTime is zero, just return the desired num for specific Num
  // if not, need to consider the current execution 
  def getNewNumber(xlist: Seq[Double], ylist: Seq[Double], midtime: Double, xNum: Double, yTime: Double): Double={
    var i = 0
    var avg: Double = 0
    var count = 0
    for (y <- ylist){
      val t = xlist(i) * 1000/ ylist(i)
      if (y < 1500){
        avg += t 
        count += 1
      }
      i += 1
    }
    return avg/count
    val oneDuration = 100
    //val mytime = midtime - oneDuration
    val mytime = midtime 
    //val (beta1, beta0) = desiredLine(xlist, ylist, mytime)
    val (beta1, beta0) = linearRegression(xlist, ylist)
    val avgX = xlist.sum / xlist.length 
    val avgY = ylist.sum / ylist.length

    if (beta1 == 0 && beta0 == 0 && xNum == 0 && yTime == 0) return minNum 
    if (beta1 == 0 && beta0 == 0) return xNum*1000/yTime
    // the range is [mytime-100, mytime+100]
    val lowY = mytime-oneDuration
    val lowX = (lowY - beta0)/beta1 
    val highY = mytime+oneDuration
    val highX = (highY - beta0)/beta1 

    val maxLen = (highX - lowX)/2
    val desiredX = (mytime - beta0)/beta1
    val pNum = avgX * 1000/avgY
    return desiredX.max(pNum)
    //xinLogInfo(s"xin PPIDRateEstimator getNewNumber for specified time $mytime, lowX $lowX, highX $highX, detectedX $desiredX")

    if (xNum > 0 && yTime > 0){
      val suppX = (yTime - beta0)/beta1
      val errorX = math.abs(suppX - xNum) 
      var k = 0.0
      if ( errorX >= maxLen ) k = 1
      else k = errorX / maxLen 
      return desiredX - k * errorX
    } else 
      return desiredX
  }
  //
  
  def updateLinearQueue(x: Double, y: Double){
    numTupleQueue.enqueue(x)
    processTimeQueue.enqueue(y)
    assert( numTupleQueue.length == processTimeQueue.length )
    if ( numTupleQueue.length > historySize ){
      numTupleQueue.dequeue()
      processTimeQueue.dequeue()
    }
  }
  def getLR(): (Double, Double)={
    linearRegression(numTupleQueue.toSeq, processTimeQueue.toSeq)
  }
 
  def compute(
      time: Long, // in milliseconds
      numElements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long // in milliseconds
    ): Option[(Long, Double, Long, Int)] = {
    //xinLogInfo(s"xin PIDRateEstimator \ntime = $time, # records = $numElements, " + s"processing time = $processingDelay, scheduling delay = $schedulingDelay")
    logTrace(s"\ntime = $time, # records = $numElements, " +
      s"processing time = $processingDelay, scheduling delay = $schedulingDelay")
    this.synchronized {
      if (time > latestTime && numElements > 0 && processingDelay > 0) {
         
        // in seconds, should be close to batchDuration
        val delaySinceUpdate = (time - latestTime).toDouble / 1000

        // in elements/second
        val processingRate = numElements.toDouble / processingDelay * 1000

        // In our system `error` is the difference between the desired rate and the measured rate
        // based on the latest batch information. We consider the desired rate to be latest rate,
        // which is what this estimator calculated for the previous batch.
        // in elements/second
        val error = latestRate - processingRate

        // The error integral, based on schedulingDelay as an indicator for accumulated errors.
        // A scheduling delay s corresponds to s * processingRate overflowing elements. Those
        // are elements that couldn't be processed in previous batches, leading to this delay.
        // In the following, we assume the processingRate didn't change too much.
        // From the number of overflowing elements we can calculate the rate at which they would be
        // processed by dividing it by the batch interval. This rate is our "historical" error,
        // or integral part, since if we subtracted this rate from the previous "calculated rate",
        // there wouldn't have been any overflowing elements, and the scheduling delay would have
        // been zero.
        // (in elements/second)
        val historicalError = schedulingDelay.toDouble * processingRate / batchIntervalMillis

        // in elements/(second ^ 2)
        val dError = (error - latestError) / delaySinceUpdate

        // xin
        //val actualRate = numElements.toDouble / batchIntervalMillis * 1000
        //var myNewRate = minRate
        val diff = (processingDelay.toDouble + schedulingDelay.toDouble - batchIntervalMillis) / batchIntervalMillis 
        
        var feedRate = diff * processingRate
        //var newRate = (latestRate - proportional * error - feedRate -
        var newRate = (latestRate - error -
                                    delayIntegral * historicalError -
                                    derivative * dError).max(minRate)
        var batchTimeStamp: Long = -1
        var nextTimestamp: Long = -1
        // negative is used to limit the number of tuples for normal jobs
        // zero is used to free the previous limitation
        // postive is used to set a specfic number of input for the a specfic job 
        var myNum: Long = -1 
        var predictedRate: Double = 0
        var tempRate: Double = 0

        val myProcessingRate = (jobNumRecords.toDouble / processingDelay * 1000)
        val myError = myLatestRate - myProcessingRate 
        val myHistoricalError = schedulingDelay.toDouble * myProcessingRate / batchIntervalMillis
        if (proportional == 6.0){
          newRate = (myLatestRate - myError - delayIntegral * myHistoricalError).max(minRate)
          if (receiverNum == -1) receiverNum = rNum 
          newRate = newRate/receiverNum
        }

        //xinLogInfo(s"xin in PIDController, start PID estimation!!! in 1")
        // xinPID   1
        if ( proportional == 6.0 && cpLenFixed == true && checkpointId == cpOffset && startPIDCollect == false){
          startPIDCollect = true  
          initializeList() 
        }

        // convert the job id to array Id
        val arrayId = ((checkpointId - cpOffset + cpLen)%cpLen).toInt
        if (startPIDCollect == true){
          if ( arrayId > lowJobNum ){
            updatePIDHistory(jobNumRecords.toDouble, processingDelay.toDouble, schedulingDelay.toDouble, checkpointId)  
          }
          updateActualHistory( processingDelay.toDouble, schedulingDelay.toDouble, checkpointId, jobNumRecords.toDouble )
          initCount += 1
        }
        // finish the collection of historical records
        // also at the end of one circle
        if (startPIDCollect == true && startPIDPredict == false && initCount >= 60 && checkpointId+1 == cpOffset){
          startPIDPredict = true
        }

        // xinPID  3 
        // xinLogInfo(s"xin PID controller 3 !!!")
        // in the mid of the circle, check the delay of the front jobs to set the Num 
        if (startPIDPredict == true && checkpointId + 1 == cpLen){
          //reductionId += 1 
          //reductionId = reductionId % delayPredictInterval 
          //if ( reductionId == 0 )

          //if ( delayTrend == false )
          delayReduction()
        }
        
        //xinLogInfo(s"xin PID controller 2 !!!")
        // xinPID  2 
        // finish one circle, check the trend to adjust the long-term rate 

        // for the delay array, one circle ends
        if (startPIDPredict == true && checkpointId + 1 == cpOffset){
          //if ( midDelay > 0.1 * batchIntervalMillis ) 
          //loadDelay()
          val myDelay = getDelayList() 
          val (lowDelay, highDelay) = getDelayTrend(myDelay)
          val midDelay = (lowDelay+highDelay)/2
          xinLogInfo(s"xin PPIDRateEstimator expectedTime $expectedTime lastDelay: $myDelay lowDelay: $lowDelay highDelay: $highDelay andMid $midDelay")

          var sumDelay: Double = 0
          for (i <- 1 to cpLen){
             sumDelay += actualDelay(i-1) 
          }
          sumDelay = sumDelay/cpLen
          avgTime = 0
          for (i <- lowJobNum.toInt to cpLen-1){
             if (actualTime(i) <= batchIntervalMillis)
               avgTime += actualTime(i) 
             else 
               avgTime += batchIntervalMillis 
          }   
          avgTime = avgTime/(cpLen-lowJobNum)
          var incP = batchIntervalMillis/avgTime
          // the start delay of next circle
          startDelay = processingDelay.toDouble + schedulingDelay.toDouble - batchIntervalMillis
          if (startDelay < 0) startDelay = 0
          val delayNum = startDelay / batchIntervalMillis
          if (receiverNum == -1) receiverNum = rNum 
          goodNum = getNewNumber(numTupleQueue, processTimeQueue, batchIntervalMillis, 0, 0) 
          xinLogInfo(s"xin PID controller calculating the goodNum, Tuples: $numTupleQueue, Time: $processTimeQueue goodNum: $goodNum lowJobNum $lowJobNum incP $incP")
          lastRNum = receiverNum
          //avgRate = (cpLen - lowJobNum - delayNum)/cpLen * goodNum * incP / receiverNum
          avgRate = (cpLen - lowJobNum - delayNum)/cpLen * goodNum / receiverNum
          xinLogInfo(s"xin PPIDRateEstimator actualTime $actualTime avgTime $avgTime actualDelay $actualDelay avgDelay $sumDelay startDelay $startDelay delayNum $delayNum avgRate $avgRate")

          if ( highDelay > 1.1*lowDelay || sumDelay > batchIntervalMillis){
             if (delayIntegral <= 0.4) delayIntegral += 0.1 
             delayTrend = true
             xinLogInfo(s"xin PPIDRateEstimator delay are increasing the delayIntegral $delayIntegral ")
          }else{
             delayTrend = false
          }

          if ( lowDelay > 1.1*highDelay && delayIntegral >= 0.3 && sumDelay < batchIntervalMillis ) delayIntegral -= 0.1
          
          clearActual() 
        }
        if ( receiverNum < lastRNum ){
          avgRate = avgRate * lastRNum / receiverNum
        }
        
        
        if ( avgRate != -1 ){
           newRate = avgRate 
        }
        
        // xinPID  4 
        // xinLogInfo(s"xin PID controller 4 !!!")
        if (startPIDPredict == true){
          // the jobId is ( the array starts at 5, plus the cpOffset )
          val jobId = checkpointId.toInt

          // in the real configuration
          // when the num is zero, directly check in the numTuplesJobs
          // otherwise need to calculate the num according to the expected time
          var jobNum:Double = -1D
          if (checkpointId >= 0 && checkpointId < cpOffset){
            val cpDelay = getCpDelayTime()
            if (checkpointId == 0){
              currentRegionDelay = cpDelay + schedulingDelay.toDouble 
              lowJobNum = 0
            }
            else if (currentRegionDelay != -1)
              currentRegionDelay += (processingDelay.toDouble - batchIntervalMillis)
            val smallTime = getSmallJobTime()
            val freeTime = batchIntervalMillis - smallTime 
            if (currentRegionDelay >= freeTime || checkpointId == 0){
              jobNum = minNum
              currentRegionDelay -= freeTime
              if (currentRegionDelay < 0) currentRegionDelay = -1
              lowJobNum += 1
              numLen = 1
            } else if (currentRegionDelay == 0 || currentRegionDelay == -1) {
              jobNum = getFullJobNum() 
              if ( numLen > 1) numLen = 0
              else numLen = 2 
              //else numLen = cpLen - checkpointId.toInt - 1 - 4
              currentRegionDelay = -1
            } else {
              val ytime = batchIntervalMillis - currentRegionDelay
              jobNum = ytime/batchIntervalMillis * getFullJobNum() 
              currentRegionDelay = -1
              numLen = 1
            }
            // set the number of NUM for mutliple jobs with numLen
            xinLogInfo(s"xin PID controller set the small job, CPdelay $cpDelay, smallTime $smallTime, freeTime $freeTime, totalDelay $currentRegionDelay, jobNum $jobNum, goodNum $goodNum")
          }
          //if (numTuplesJobs( jobId ) == minNum) jobNum = minNum 
          //else jobNum = getJobNum( jobId )
          //val etime = expectedTime( jobId )
          //xinLogInfo(s"xin PID controller potential num of the job: $jobNum the time $etime, goodNum $goodNum numReciever $receiverNum")
          if ( jobNum != goodNum ) {
            xinLogInfo(s"xin PID controller check the source Tuples: $numTupleQueue, Time: $processTimeQueue, jobNum $jobNum, goodNum $goodNum")
          }
          // val rate = getNewNumber(numTupleQueue, processTimeQueue, expectedTime(checkpointId.toInt), numElements.toDouble, processingDelay.toDouble)/receiverNum
          // note the rate is for the whole job. Need to divide rate by the number of receiver
          if (checkpointId >= 0 && checkpointId < cpOffset){
            if (jobNum >= minNum){
              if ( jobNum == minNum ) myNum = minNum.toLong
              else myNum = jobNum.toLong/6 
              val startTime = (time - processingDelay - schedulingDelay)/100
                batchTimeStamp = startTime * 100 
                nextTimestamp = batchTimeStamp + 5000
              if (startTime % 10 != 0){
                xinLogInfo(s"xin PID controller the starting Timestamp might be wrong: $startTime")
              }
              //if (checkpointId + 1 == cpOffset) lastSetNum = jobNum
              //if (checkpointId == 0 && jobNumRecords.toDouble > lastSetNum * 1.1) delayIntegral += 0.1
            }
          } 
          
          xinLogInfo(s"xin PPIDRateEstimator expectedTime: $expectedTime jobID $jobId Num: $jobNum actualRate $newRate integral $delayIntegral $numLen #receivers $receiverNum")
        }
        // xinLogInfo(s"xin PID controller the end of estimation !!!")
        // xinPID

        // modification starts
        if ( proportional > 1.0 && proportional < 5 ){
           
	tempRate = newRate 
	//tempRate = newRate * proportional 
        // using the total number of records per job
        updateLinearQueue(jobNumRecords.toDouble, processingDelay.toDouble)
        val (beta1, beta0) = getLR()
        //y=x*beta1 + beta0
        if ( beta0 > 0 && numTupleQueue.length >= historySize){
          predictedRate = (1000-beta0)/beta1
          predictedRate = predictedRate / receiverNum
          //predictedRate = predictedRate - integral * historicalError

          //handling throughput
          if ( proportional == 2.0 || proportional == 4)newRate = predictedRate

          mQueue.enqueue( newRate )
          if ( mQueue.length > historySize )
            mQueue.dequeue()
        }
        
        val startTime = (time - processingDelay - schedulingDelay)/100
        if (startTime % 10 == 0){
          batchTimeStamp = startTime * 100 
        } 

        // handling scheduling delay 
        if ( proportional == 3.0 || proportional == 4 ){
        // set the predicted job to 0, if detected to be the checkpointing job
        if (checkpointId == 0){ 
          lastCpRate = jobNumRecords.toDouble * 1000/ (processingDelay*6) 
          if ( batchTimeStamp != -1 ){
            nextTimestamp = batchTimeStamp + 5000  
          }
          //newRate = minRate 
          myNum = 0 
          lastEmpty = true 
        } 

        // a threshold for the delay time of the next job
        /*
        val intervalM = 0.8 * batchIntervalMillis
        val nextDelay = schedulingDelay + processingDelay - batchIntervalMillis
        if (nextDelay >= intervalM && lastEmpty == false) {
          if ( batchTimeStamp != -1 ){
            lastEmpty = true 
            //nextTimestamp = batchTimeStamp + 3000  
            nextTimestamp = (batchTimeStamp + schedulingDelay + processingDelay + batchIntervalMillis)/1000 * 1000
            // move 1 step forward if the next job is the detected checkpointing job, by coincidence
            if ( ((nextTimestamp - batchTimeStamp)/1000 + checkpointId) % 10 == 0 )
              nextTimestamp = nextTimestamp + batchIntervalMillis 
            // next time it does not check the next job
            if ( nextDelay >= 2*batchIntervalMillis )
              lastEmpty = false 
          }
          myNum = 0
        }
        */

        // this job is right after the checkpointing job
        // set a maximum number as negative, because it accumulates the tuples
        // that should be processed in the last job
        if (checkpointId >= 1 && checkpointId<=4){
          if ( batchTimeStamp != -1 ){
            nextTimestamp = batchTimeStamp + 5000  
          }
          // negative is used as a threshold
          //myNum = -1 * (predictedRate*1.5/5).toLong
          //myNum = -1 * (rateQueue.reduceLeft(_ max _)/5).toLong
          //myNum = -1 * (maxRate/5).toLong
          if ( mQueue.isEmpty ){
            //myNum = -1 * 5000 
          }
          else
            //myNum = -1 * (mQueue.reduceLeft(_ max _)/5).toLong
            myNum = (mQueue.reduceLeft(_ max _)).toLong
        }  
        if (lastJobId + 1 != checkpointId){
          lastEmpty = false
        } 
        } 
        }

        logTrace(s"""
            | latestRate = $latestRate, error = $error
            | latestError = $latestError, historicalError = $historicalError
            | delaySinceUpdate = $delaySinceUpdate, dError = $dError
            """.stripMargin)
        
        //xinLogInfo(s"xin PIDRateEstimator \ntime = $time, #records = $numElements, " + s"processing time = $processingDelay, scheduling delay = $schedulingDelay, error = $error, myfeedRate = $feedRate, rate: $latestRate to $newRate ")
        xinLogInfo(s"xin PIDRateEstimator time = $time, #records = $numElements, " + s"processing time = $processingDelay, scheduling delay = $schedulingDelay, processingRate = $processingRate, rate: $latestRate to $newRate $checkpointId Time $batchTimeStamp $jobNumRecords $myNum $predictedRate")

        latestTime = time
        if (firstRun) {
          
          latestRate = processingRate
          myLatestRate = myProcessingRate
          latestError = 0D
          firstRun = false
          lastJobId = checkpointId 
          logTrace("First run, rate estimation skipped")
          None
        } else {
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some((nextTimestamp, newRate, myNum, numLen))
        }
      } else {
        logTrace("Rate estimation skipped")
        xinLogInfo(s"xin in PIDController, rate estimation skipped")
        None
      }
    }
  }
}
