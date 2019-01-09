package com.alex.offline.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alex.offline.bean.SessionInfo
import com.alex.sparkmall.common.bean.UserVisitAction
import com.alex.sparkmall.common.utils.ConfigurationUtil
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SessionExtractorApp {
  def extractSession(sessionIdGroupByKey: RDD[(String, Iterable[UserVisitAction])],taskID:String,sparkSession:SparkSession):Unit = {
    //需求二

    //按每小时session数量比例随机抽取1000个session
    // 1 把 session 的动作集合 整理成指定要求的sessionInfo
    // 2 用天+小时作为key 进行聚合 RDD[dayhourkey,sessionInfo] =>groupbykey=> RDD[dayhourkey,iterable[sessionInfo]]
    // 3 根据公式 计算出每小时要抽取session个数，
    //flatmap
    // 每小时要抽取session个数=本小时session个数 / 总session数 * 要抽取的总session数
    // 4 用这个个数从session集合中抽取相应session。
    //5 保存到mysql中


    //整理info
    val sessioninfo: RDD[SessionInfo] = sessionIdGroupByKey.map { case (sessionid, userAction) =>

      var min = 0L
      var max = 0L
      var searchKeywordList = new ListBuffer[String]()
      var clickProductIdsList = new ListBuffer[String]()
      var orderProductIdsList = new ListBuffer[String]()
      var payProductIdsList = new ListBuffer[String]()


      userAction.foreach(x => {

        val time: Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(x.action_time).getTime


        if (min != 0) {
          min = math.min(time, min)
        } else {
          min = time
        }

        if (max != 0) {
          max = math.max(time, max)
        } else {
          max = time
        }
        if (x.search_keyword != null) searchKeywordList += x.search_keyword
        if (x.click_product_id != -1L) clickProductIdsList += x.click_product_id.toString
        if (x.order_product_ids != null) orderProductIdsList += x.order_product_ids
        if (x.pay_product_ids != null) payProductIdsList += x.pay_product_ids
      })

      val visitlength = max - min
      var visitstep = userAction.size

      val searchKeywords = searchKeywordList.mkString(",")
      val clickProductIds = clickProductIdsList.mkString(",")
      val orderProductIds = orderProductIdsList.mkString(",")
      val payProductIds = payProductIdsList.mkString(",")
      val startTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(min))

      SessionInfo(taskID, sessionid, startTime, visitstep, visitlength, searchKeywords, clickProductIds, orderProductIds, payProductIds)
    }

    //转换为(time,SessionInfo),为groupbykey准备
    //"yyyy-MM-dd HH:mm:ss"  按照:分割 ,保留yyyy-MM-dd HH:部分
    val forgroupBytime: RDD[(String, SessionInfo)] = sessioninfo.map(x => (x.startTime.split(":")(0), x))

    //按照time分割 (time,Iterable[SessionInfo]))
    val sessioninfoGroupByKey: RDD[(String, Iterable[SessionInfo])] = forgroupBytime.groupByKey()

    val Allsize = 1000

    val sessioncount: Long = sessionIdGroupByKey.count()


    val infoResList = new ListBuffer[SessionInfo]()

    println("==================================")
    //println(sessioninfoGroupByKey.take(1))
    //注意这里要用flatmap,如果用map 返回值就是RDD[ListBuffer[SessionInfo]] ,还需要再取一遍
    val infores2sql: RDD[SessionInfo] = sessioninfoGroupByKey.flatMap { case (time, info) =>

      val currCount: Int = info.size
      val currRat: Long = math.round(currCount.toDouble * Allsize / sessioncount)

      var curset = new mutable.HashSet[SessionInfo]()
      val rawlist = info.toList


      //随机抽数
      while (curset.size < currRat) {

        val index: Int = new Random().nextInt(rawlist.size)
        curset += rawlist(index)

      }

      infoResList ++= curset.toList


      //      val sessionList: List[SessionInfo] = extractNum(info.toArray,currRat.toInt)
      //      sessionList
      infoResList
      // curset.toList
    }
    //    println(infores2sql.collect().size)
    //    println(infores2sql.take(5).mkString(","))


    //5 保存到mysql中
    import sparkSession.implicits._

    val config2: FileBasedConfiguration = ConfigurationUtil("config.properties").config
    infores2sql.toDF.write.format("jdbc")
      .option("url", config2.getString("jdbc.url"))
      .option("user", config2.getString("jdbc.user"))
      .option("password", config2.getString("jdbc.password"))
      .option("dbtable", "random_session_info").mode(SaveMode.Append).save()
    //config2
  }


  def extractNum[T](sourceList: Array[T], num:Int): List[T] ={

    val resultBuffer = new ListBuffer[T]()

    val indexSet = new mutable.HashSet[Int]()

    while(resultBuffer.size<num){
      // 先生成随机下标
      val index: Int = new Random().nextInt(sourceList.size)
      // 判断新产生的下标是否已经使用过
      if( ! indexSet.contains(index)){
        resultBuffer+=sourceList(index)
        indexSet+=index
      }
    }
    resultBuffer.toList
  }

}
