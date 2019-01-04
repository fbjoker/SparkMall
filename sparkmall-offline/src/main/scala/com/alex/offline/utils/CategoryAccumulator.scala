package com.alex.offline.utils

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable
import scala.collection.mutable._

class CategoryAccumulator  extends  AccumulatorV2[String,HashMap[String,Long]]{

      var categoryCount = new  HashMap[String,Long]()

  override def isZero: Boolean = categoryCount.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val accumulator = new CategoryAccumulator()
    accumulator.categoryCount ++=this.categoryCount
    accumulator
  }

  override def reset(): Unit = new  HashMap[String,Long]()

  override def add(key: String): Unit = {

    categoryCount(key)=categoryCount.getOrElse(key,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {

    val othervalue: mutable.HashMap[String, Long] = other.value

    val res: mutable.HashMap[String, Long] = this.categoryCount.foldLeft(othervalue) { case (sessionOther: mutable.HashMap[String, Long], (key,count)) =>

      othervalue(key) = othervalue.getOrElse(key,0L) + count
      othervalue
    }

    this.categoryCount=res
  }

  override def value: mutable.HashMap[String, Long] = {
    categoryCount
  }
}
