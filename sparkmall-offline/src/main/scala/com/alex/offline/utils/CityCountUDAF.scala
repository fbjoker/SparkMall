package com.alex.offline.utils




import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction, UserDefinedFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
class CityCountUDAF  extends  UserDefinedAggregateFunction{
  //定义输入参数和数据类型Array(StructField("city_name",StringType)
  override def inputSchema: StructType = StructType(StructField("city_name",StringType)::Nil)

  //缓存的数据类型
  override def bufferSchema: StructType = StructType(Array(StructField("cityCount",MapType(StringType,LongType)),StructField("totalCityCount",LongType)))

  //最终的输出类型
  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  //初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new HashMap()
    buffer(1)=0L
  }
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inputcity: String = input.getString(0)


    val citymap: Map[String, Long] = buffer.getAs[Map[String,Long]](0)

    buffer(0)= citymap(inputcity)+citymap(inputcity)+1L

    buffer(1)=buffer.getLong(1)+1L


  }

  //把各个分区内的数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???

  override def evaluate(buffer: Row): Any = ???
}
