package com.alex.offline.app

import java.util.Properties

import com.alex.offline.bean.{Categoryinfo, Top10info}
import com.alex.sparkmall.common.bean.UserVisitAction
import com.alex.sparkmall.common.utils.ConfigurationUtil
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object CategoryTop10SessionApp {
   def statCategoryTop10Session(sparkSession: SparkSession, filterRDD: RDD[UserVisitAction], sortedCategoryinfo: List[Categoryinfo],taskID:String): Unit = {
    //需求四
    //对于排名前 10 的品类，分别获取其点击次数排名前 10 的 sessionId。

    //首先从满足条件的数据中,filter出在排名前十中的数据,把排名前十品类放在广播变量中

    val sortedCategoryinfobc: Broadcast[List[Categoryinfo]] = sparkSession.sparkContext.broadcast(sortedCategoryinfo)

    //把前10品类的数据过滤出来
    val filteredAction: RDD[UserVisitAction] = filterRDD.filter(uservisitaciton => {

      var flag = false

      for (elem <- sortedCategoryinfobc.value) {

        if (uservisitaciton.click_category_id.toString == elem.categoryId) {

          flag = true
        }
      }
      flag

    })

    //把数据按照(品类+sessionid, 1L)这种格式转换, 方便统计统一品类下,同一个session的个数, 然后通过reducebykey聚合(品类+sessionid, 点击次数)
    val cidAndsessionidAndcount: RDD[(String, Long)] = filteredAction.map(uservisitaction => (uservisitaction.click_category_id + "_" + uservisitaction.session_id, 1L)).reduceByKey(_ + _)


    //转换为(品类,(sessionid,数量)) ,然后再按照品类聚合(品类,Iterable(sessionid,数量))
    val cidAndsessioncountgroupbykey: RDD[(String, Iterable[(String, Long)])] = cidAndsessionidAndcount.map { case (cidandsessionid, count) => {
      (cidandsessionid.split("_")(0), (cidandsessionid.split("_")(1), count))

    }
    }.groupByKey()

    //(品类,Iterable(sessionid,数量))按照数量进行排序取前10       //把结果压平方便存入sql
    val Top10res: RDD[Top10info] = cidAndsessioncountgroupbykey.flatMap { case (cid, sessionidAndcount) => {
      val cidtop10session: List[(String, Long)] = sessionidAndcount.toList.sortWith { (a, b) => {
        a._2 > b._2

        //如果用if

      }
      }.take(10)
      //把结果直接转换为Top10info对象,方便直接存入sql
      cidtop10session.map { x =>
        Top10info(taskID, cid, x._1, x._2)

      }

    }

    }
     import sparkSession.implicits._

    //    Top10res.toDF.write.format("jdbc")
    //      .option("url", config2.getString("jdbc.url"))
    //      .option("user", config2.getString("jdbc.user"))
    //      .option("password", config2.getString("jdbc.password"))
    //      .option("dbtable", "top10").mode(SaveMode.Append).save()


    val config: FileBasedConfiguration = ConfigurationUtil("config.properties").config


    val properties = new Properties()
    properties.put("user", config.getString("jdbc.user"))
    properties.put("password", config.getString("jdbc.password"))


    Top10res.toDF.write.mode(SaveMode.Append).jdbc(config.getString("jdbc.url"), "top10", properties)
  }


}

