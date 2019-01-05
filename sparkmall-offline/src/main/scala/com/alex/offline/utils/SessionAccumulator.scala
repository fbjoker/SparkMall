package com.alex.offline.utils

import org.apache.spark.util.AccumulatorV2

import collection.mutable._
import scala.collection.mutable

class SessionAccumulator  extends  AccumulatorV2[String,HashMap[String,Long]]{

      var seesionCount = new  HashMap[String,Long]()

  override def isZero: Boolean = seesionCount.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new SessionAccumulator()
    //源码里自带的累加器都是这么写的,传值过去
    accumulator.seesionCount ++=this.seesionCount
    accumulator
  }

  override def reset(): Unit = new  HashMap[String,Long]()

  override def add(key: String): Unit = {

    seesionCount(key)=seesionCount.getOrElse(key,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    val othervalue: mutable.HashMap[String, Long] = other.value

    val res: mutable.HashMap[String, Long] = this.seesionCount.foldLeft(othervalue) {
      case (sessionOther, (key,count)) =>

        sessionOther(key) = othervalue.getOrElse(key,0L) + count
        sessionOther
    }

    this.seesionCount=res
  }

  override def value: mutable.HashMap[String, Long] = {
    seesionCount
  }
}
