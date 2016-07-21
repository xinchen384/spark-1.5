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
  private val cpLen = 10
  private var checkpointId: Double = -1L 
  private var receiverNum: Double = -1L 
  private var jobNumRecords: Double = -1L 
  private var lastCpRate: Double = -1L 
  private var myMinRate: Double = -1L 
  private var cplist = new ListBuffer[Ele]()

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
    ): Option[(Long, Double, Long)] = {
      checkpointId = Id
      receiverNum = num
      jobNumRecords = totalEle
      compute(time, numElements, processingDelay, schedulingDelay)
  }
  class Ele(var delay: Long, var num_records: Long, var ptime: Long) extends Serializable{
    var max_rate: Long = num_records/ptime * 1000
    var min_rate: Long = num_records/ptime * 1000

    def updateJob(mydelay: Long, mynum: Long, mytime: Long){
      delay = mydelay
      num_records = mynum
      ptime = mytime
      val proRate: Long = (mynum.toFloat/mytime.toFloat*1000).toLong
      if (ptime < batchIntervalMillis && proRate > max_rate){
        max_rate = proRate 
      }
      if (proRate < min_rate){
        min_rate = proRate 
      }
    }
    override def toString(): String = "(" + delay + ", " + num_records + ", " + ptime + ")";
  }

  def linearRegression(xlist: Seq[Double], ylist: Seq[Double]): (Double, Double)={
    val xbar = xlist.sum / xlist.length
    val ybar = ylist.sum / ylist.length
    val xx_bar = xlist.map( x => (x-xbar)*(x-xbar) ).sum
    val xy_bar = (xlist zip ylist).map{case (x,y) => (x-xbar)*(y-ybar) }.sum
    val beta1 = xy_bar/xx_bar
    return (beta1, ybar-beta1*xbar)
  }
  var lastEmpty = false
  var lastJobId: Double = -1D
  val xQueue = new Queue[Double]
  val yQueue = new Queue[Double]
  val mQueue = new Queue[Double]
  val queueSize = 10
  def updateLinearQueue(x: Double, y: Double){
    xQueue.enqueue(x)
    yQueue.enqueue(y)
    assert( xQueue.length == yQueue.length )
    if ( xQueue.length > queueSize ){
      xQueue.dequeue()
      yQueue.dequeue()
    }
  }
  def getLR(): (Double, Double)={
    linearRegression(xQueue.toSeq, yQueue.toSeq)
  }
 
  def compute(
      time: Long, // in milliseconds
      numElements: Long,
      processingDelay: Long, // in milliseconds
      schedulingDelay: Long // in milliseconds
    ): Option[(Long, Double, Long)] = {
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
        //if ( schedulingDelay.toDouble == 0 ){
        //  if ( diff <= 0 ){
        //    if (diff < -0.2){
        //      if (myMinRate < processingRate)
        //        myMinRate = processingRate
        //      myNewRate = processingRate.max(myMinRate)
        //    } else{
        //      myNewRate = actualRate
        //    }
        //  }else{
        //    if (diff < 0.1) myNewRate = myMinRate.min(actualRate).max(minRate) 
        //    else myNewRate = minRate 
        //  }
        //}else{
        //  if (diff <= 0){
        //    if ( diff < -0.2 ){
        //      myNewRate = (latestRate - diff * processingRate).max(processingRate).max(minRate)
        //    }else{
        //      myNewRate = processingRate
        //    }
        //  } else{
        //    // directly going to minimal because we think it's caused by CP 
        //    myNewRate = minRate
        //  } 
        //}
        //val newRate = myNewRate

        var feedRate = diff * processingRate
        //var newRate = (latestRate - proportional * error - feedRate -
        var newRate = (latestRate - error -
                                    integral * historicalError -
                                    derivative * dError).max(minRate)
        var batchTimeStamp: Long = -1
        var nextTimestamp: Long = -1
        // negative is used to limit the number of tuples for normal jobs
        // zero is used to free the previous limitation
        // postive is used to set a specfic number of input for the a specfic job 
        var myNum: Long = -1 
        var predictedRate: Double = 0
        var tempRate: Double = 0

        // modification starts
        if ( proportional > 1.0 ){

	tempRate = newRate 
	//tempRate = newRate * proportional 
        // using the total number of records per job
        updateLinearQueue(jobNumRecords.toDouble, processingDelay.toDouble)
        val (beta1, beta0) = getLR()
        //y=x*beta1 + beta0
        if ( beta0 > 0 && xQueue.length >= queueSize){
          predictedRate = (1000-beta0)/beta1
          predictedRate = predictedRate / receiverNum
          //predictedRate = predictedRate - integral * historicalError

          //handling throughput
          if ( proportional == 2.0 || proportional >= 4)newRate = predictedRate

          mQueue.enqueue( newRate )
          if ( mQueue.length > queueSize )
            mQueue.dequeue()
        }
        

        val startTime = (time - processingDelay - schedulingDelay)/100
        if (startTime % 10 == 0){
          batchTimeStamp = startTime * 100 
        } 

        // handling scheduling delay 
        if ( proportional == 3.0 || proportional >= 4 ){
        if (checkpointId == 0){ 
          lastCpRate = jobNumRecords.toDouble * 1000/ (processingDelay*6) 
          if ( batchTimeStamp != -1 ){
            nextTimestamp = batchTimeStamp + 5000  
          }
          //newRate = minRate 
          myNum = 0 
          lastEmpty = true 
        } 
        val intervalM = 0.8 * batchIntervalMillis
        val nextDelay = schedulingDelay + processingDelay - batchIntervalMillis
        if (nextDelay >= intervalM && lastEmpty == false) {
          if ( batchTimeStamp != -1 ){
            lastEmpty = true 
            //nextTimestamp = batchTimeStamp + 3000  
            nextTimestamp = (batchTimeStamp + schedulingDelay + processingDelay + batchIntervalMillis)/1000 * 1000
            if ( ((nextTimestamp - batchTimeStamp)/1000 + checkpointId) % 10 == 0 )
              nextTimestamp = nextTimestamp + batchIntervalMillis 
            if ( nextDelay >= 2*batchIntervalMillis )
              lastEmpty = false 
          }
          myNum = 0
        }
        if (checkpointId == 1){
          if ( batchTimeStamp != -1 ){
            nextTimestamp = batchTimeStamp + 5000  
          }
          // negative is used as a threshold
          //myNum = -1 * (predictedRate*1.5/5).toLong
          //myNum = -1 * (rateQueue.reduceLeft(_ max _)/5).toLong
          //myNum = -1 * (maxRate/5).toLong
          if ( mQueue.isEmpty )
            myNum = -1 * 5000 
          else
            myNum = -1 * (mQueue.reduceLeft(_ max _)/5).toLong
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
        xinLogInfo(s"xin PIDRateEstimator time = $time, #records = $numElements, " + s"processing time = $processingDelay, scheduling delay = $schedulingDelay, processingRate = $processingRate, rate: $latestRate to $newRate $checkpointId Time $batchTimeStamp $jobNumRecords $tempRate $predictedRate")

        latestTime = time
        if (firstRun) {
          for ( i <- 1 to cpLen){
            cplist += new Ele(-1, 10, 1)
          }
          latestRate = processingRate
          latestError = 0D
          firstRun = false
          lastJobId = checkpointId 
          logTrace("First run, rate estimation skipped")
          None
        } else {
          cplist.lift(checkpointId.toInt).get.updateJob(schedulingDelay, numElements, processingDelay)
          latestRate = newRate
          latestError = error
          logTrace(s"New rate = $newRate")
          Some((nextTimestamp, newRate, myNum))
        }
      } else {
        logTrace("Rate estimation skipped")
        None
      }
    }
  }
}
