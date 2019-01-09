package com.alex.offline

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties, UUID}

import com.alex.offline.app.{CategoryTop10App, CategoryTop10SessionApp, SessionExtractorApp, SessionStatApp}
import com.alex.offline.bean.{Categoryinfo, SessionInfo, Top10info}
import com.alex.offline.utils.{CategoryAccumulator, SessionAccumulator}
import com.alex.sparkmall.common.bean.UserVisitAction
import com.alex.sparkmall.common.utils.{ConfigurationUtil, JdbcUtil}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.{immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.util.Random

object OfflineApp {

  var SESSIONCOUNT = 0L

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("offline")


    val sparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()
    import sparkSession.implicits._


    //处理批次
    val taskID: String = UUID.randomUUID().toString


    //读取配置文件
    val config: FileBasedConfiguration = ConfigurationUtil("condition.properties").config

    val jsonstr: String = config.getString("condition.params.json")

    //使用阿里的json解析工具,解析json字符串
    val conditionJSOBJ: JSONObject = JSON.parseObject(jsonstr)

    //    val teststr: String = conditionJSOBJ.getString("endAge")
    //    println(teststr)

    //筛选过后满足条件的RDD
    val filterRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession, conditionJSOBJ)
    //把数据转换成为(sessionid ,UserVisitAction ) 格式的方便按照key聚合
    val sessionidAndUserVisitAction: RDD[(String, UserVisitAction)] = filterRDD.map(x => (x.session_id, x))

    //按照key进行聚合(sessionid,Iterable[UserVisitAction])
    val sessionIdGroupByKey: RDD[(String, Iterable[UserVisitAction])] = sessionidAndUserVisitAction.groupByKey()


    val time0: Long = System.currentTimeMillis()
    //需求一
    SessionStatApp.statsession(sparkSession, taskID, jsonstr, sessionIdGroupByKey)

    val time1: Long = System.currentTimeMillis()
    println("需求一完成")
    //需二
    SessionExtractorApp.extractSession(sessionIdGroupByKey, taskID, sparkSession)

    val time2: Long = System.currentTimeMillis()
    println("需求二完成")
    //需求三
    val sortedCategoryinfo: List[Categoryinfo] = CategoryTop10App.statCategoryTop10(sparkSession, filterRDD, taskID)

    val time3: Long = System.currentTimeMillis()
    println("需求三完成")

    //需求四
    CategoryTop10SessionApp.statCategoryTop10Session(sparkSession, filterRDD, sortedCategoryinfo,taskID)

    val time4: Long = System.currentTimeMillis()
    println("需求四完成")

    println("需求一完成时间:"+(time1-time0))
    println("需求二完成时间:"+(time2-time1))
    println("需求三完成时间:"+(time3-time2))
    println("需求四完成时间:"+(time4-time3))


    //获取要统计的页面
    val targetPagestr: Array[String] = conditionJSOBJ.getString("targetPageFlow").split(",")
    //最后一个元素不要
    val targetpage: Array[String] = targetPagestr.slice(0, targetPagestr.length - 1)

    //通过zip函数把 1,2,3,4  (1,2),(2,3),(3,4)转换为1-2,2-3,3-4的格式
    val targetPageArry: Array[String] = targetpage.zip(targetpage.slice(1, targetpage.length - 1)).map {
      case (page1, page2) => page1 + "_" + page2
    }

    //注册广播变量
    val targetpagebc: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetpage)
    val targetPageArrybc: Broadcast[Array[String]] = sparkSession.sparkContext.broadcast(targetPageArry)

    //把满足条件的数据过滤出来, 并按照pageid 进行统计,得到每一步的用户   countByKey是一个action算子
    val pageCount: collection.Map[Long, Long] = filterRDD.filter(useraction=> targetpagebc.value.contains( useraction.page_id.toString) )
                                                          .map(useraction=> (useraction.page_id,1L)).countByKey()



    //统计相邻步的用户
    //首先按照(sessionid,useraction) 格式转换,然后再bykey  得到(sessionid,useractionIterable)
    val sessionAnduseranction: RDD[(String, Iterable[UserVisitAction])] = filterRDD.map( useraction=> (useraction.session_id,useraction)).groupByKey()



    // 把同一个sessionid的action按照时间进行排序, 是对value进行sort
    val steppage: RDD[String] = sessionAnduseranction.flatMap { case (sessionid, action) =>
      //把排好序的页面号取出来, 方便以后的zip
      val pageid: List[Long] = action.toList.sortWith((a, b) => a.action_time < b.action_time).map(_.page_id)

      //把取出来的  1,2,3,4  (1,2),(2,3),(3,4)转换为1-2,2-3,3-4的格式
      val pageidmerge: List[String] = pageid.zip(pageid.slice(1, pageid.length)).map { case (page1, page2) => page1 + "_" + page2 }

      //过滤在统计要求里的数据

      val respage: List[String] = pageidmerge.filter(page => targetPageArrybc.value.contains(page))


      respage
    }
    //相邻步的用户统计
    val setppageCount: RDD[(String, Long)] = steppage.map((_,1L)).reduceByKey(_+_)


    //计算比率
    val restosql: RDD[Array[Any]] = setppageCount.map { case (pagestep, count) =>

      val rate: Double = count.toDouble / pageCount.getOrElse(pagestep.split("_")(0).toLong, 0L)


      Array(taskID, pagestep, rate)
    }
   // restosql



    println(restosql.take(1).mkString("\n"))
    restosql.take(1).foreach(println)




  }



  def readUserVisitActionRDD(sparkSession: SparkSession, conditionJsonOBJ:JSONObject):RDD[UserVisitAction]={

    //where 1=1 是一个技巧,后面都可以加and了, 而不用考虑是where还是and
    //这里使用v.* 另外一张表的已经在条件where后面用过了,而返回的数据字需要v表的就可以, 正常应该把需要的字段都写出来
    var sql = " select v.* from  user_visit_action v  join user_info u  on v.user_id=u.user_id where 1=1 "

    if(conditionJsonOBJ.getString("startDate")!=null&&conditionJsonOBJ.getString("startDate").length>0){

      sql+="and date > '"+conditionJsonOBJ.getString("startDate")
    }
    if(conditionJsonOBJ.getString("endDate")!=null&&conditionJsonOBJ.getString("endDate").length>0){

      sql+=" ' and date < '"+conditionJsonOBJ.getString("endDate")
    }
    if(conditionJsonOBJ.getString("startAge")!=null&&conditionJsonOBJ.getString("startAge").length>0){

      sql+="' and u.age >"+conditionJsonOBJ.getString("startAge")
    }
    if(conditionJsonOBJ.getString("endAge")!=null&&conditionJsonOBJ.getString("endAge").length>0){

      sql+=" and u.age <"+conditionJsonOBJ.getString("endAge")
    }


    sparkSession.sql("use sparkmall")
   // println(sql)
    import sparkSession.implicits._

    //  println(sparkSession.sql(sql).as[UserVisitAction].rdd.take(1).mkString(""))
    //println(sql)
    sparkSession.sql(sql).as[UserVisitAction].rdd

  }

}

