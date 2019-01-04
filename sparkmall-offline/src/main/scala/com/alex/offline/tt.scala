package com.alex.offline

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.alex.sparkmall.common.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object tt {
  def main(args: Array[String]): Unit = {


    // val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("tt")


    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("tt")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    spark.sql("use sparkmall")
    val userVisit: RDD[UserVisitAction] = spark.sql("select * from user_visit_action limit ").as[UserVisitAction].rdd
    val userVistMap: RDD[(String, UserVisitAction)] = userVisit.map(x => (x.session_id, x))
    val userVistGroupByKey: RDD[(String, Iterable[UserVisitAction])] = userVistMap.groupByKey()





    //    val com = (in: Iterable[String]) => {
//      var max = simpleDateFormat.parse("2010-11-26 17:37:43")
//      var min = simpleDateFormat.parse("2020-11-26 17:37:43")
//      in.map(item => {
//        val date: Date = simpleDateFormat.parse(item)
//        if (date.getTime > max.getTime) {
//          max = date
//        }
//        if (date.getTime < min.getTime) {
//          min = date
//        }
//
//
//      })
//
//      max.getTime - min.getTime
//
//    }
val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


    val res: RDD[(String, Long)] = userVistGroupByKey.map(elem => {
      // println(elem)
      // elem._2.foreach(x=>println(x.action_time))
      // val strings: Iterable[String] = elem._2.map(_.action_time)
      var max = simpleDateFormat.parse("2010-11-26 17:37:43")
      var min = simpleDateFormat.parse("2020-11-26 17:37:43")
      elem._2.map(item => {
        val date: Date = simpleDateFormat.parse(item.action_time)
        if (date.getTime > max.getTime) {
          max = date
        }
        if (date.getTime < min.getTime) {
          min = date
        }


      })

      (elem._1, max.getTime - min.getTime)

    })





    println(res.collect().size)
    println(res.collect().filter(_._2>10*1000).size)
    println(res.collect().take(5).mkString(" "))

//    for (elem <- userVistGroupByKey.take(100)) {
//      println(elem._1)
//
//    }

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "root123")

   val tosql=Array(1,33)
      val df: DataFrame = spark.sparkContext.parallelize(tosql).map(x=>Tosql(x.toString, "99")).toDF()

  // df.write.jdbc("jdbc:mysql://localhost:3306/Spark", "visitinfo1", connectionProperties)



  spark.stop()

}

}

case  class  Tosql(a:String,b:String)