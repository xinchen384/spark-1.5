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

package org.apache.spark.streaming.receiver

import com.google.common.util.concurrent.{RateLimiter => GuavaRateLimiter}
//import org.apache.spark.streaming.util.{RateLimiter => GuavaRateLimiter}
import org.apache.spark.{Logging, SparkConf}
import scala.collection.mutable

/** Provides waitToPush() method to limit the rate at which receivers consume data.
  *
  * waitToPush method will block the thread if too many messages have been pushed too quickly,
  * and only return when a new message has been pushed. It assumes that only one message is
  * pushed at a time.
  *
  * The spark configuration spark.streaming.receiver.maxRate gives the maximum number of messages
  * per second that each receiver will accept.
  *
  * @param conf spark configuration
  */
private[receiver] abstract class RateLimiter(conf: SparkConf) extends Logging {

  // treated as an upper limit
  private val maxRateLimit = conf.getLong("spark.streaming.receiver.maxRate", Long.MaxValue)
  //private lazy val rateLimiter = GuavaRateLimiter.create(maxRateLimit.toDouble, 1, TimeUnit.SECONDS)
  private lazy val rateLimiter = GuavaRateLimiter.create(maxRateLimit.toDouble)

  def waitToPush() {
    rateLimiter.acquire()
  }

  /**
   * Return the current rate limit. If no limit has been set so far, it returns {{{Long.MaxValue}}}.
   */
  def getCurrentLimit: Long = rateLimiter.getRate.toLong
  //xin
  case class BatchNum(jobTime: Long, var numRecords: Long, blockNum: Long)
  val rateQueue = new mutable.Queue[BatchNum]
  val timestampRates = new mutable.HashMap[Long, Long]
  //val maxS
  var maxNumBlock: Long = -1L

  /**
   * Set the rate limit to `newRate`. The new rate will not exceed the maximum rate configured by
   * {{{spark.streaming.receiver.maxRate}}}, even if `newRate` is higher than that.
   *
   * @param newRate A new rate in events per second. It has no effect if it's 0 or negative.
   */
  private[receiver] def updateRate(time: Long, newRate: Long, num: Long, len: Int): Unit = {
    if (newRate > 0) {
      if (maxRateLimit > 0) {
        rateLimiter.setRate(newRate.min(maxRateLimit))
      } 

      if (time > 0){
        if (num >= 0){
          //rateQueue.enqueue( new BatchNum(time, num, 0) ) 
          var tempTime = time
          for ( i <- 1 to len ){
            if ( timestampRates.contains(tempTime) ){
              xinLogInfo(s"xin Ratelimiter UpdateTime: $time repeat the timestamp")
            } else {
              timestampRates(tempTime) = num
            } 
            tempTime += 1000
          }
          if ( len > 1 ) maxNumBlock = num/5
        } 
        xinLogInfo(s"xin Ratelimiter UpdateTime: $time newRate $newRate number $num with Len: $len")
      }
    }
  }
}
