package com.alex.offline

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}

import com.alex.offline.bean.SessionInfo
import com.alex.offline.utils.SessionAccumulator
import com.alex.sparkmall.common.bean.UserVisitAction
import com.alex.sparkmall.common.utils.{ConfigurationUtil, JdbcUtil}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.commons.configuration2.FileBasedConfiguration
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

object OfflineApp {

  var SESSIONCOUNT=0L

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
    val filterRDD: RDD[UserVisitAction] = readUserVisitActionRDD(sparkSession,conditionJSOBJ)
//把数据转换成为(sessionid ,UserVisitAction ) 格式的方便按照key聚合
    val sessionidAndUserVisitAction: RDD[(String, UserVisitAction)] = filterRDD.map(x=>(x.session_id,x))

    //按照key进行聚合(sessionid,Iterable[UserVisitAction])
    val sessionIdGroupByKey: RDD[(String, Iterable[UserVisitAction])] = sessionidAndUserVisitAction.groupByKey()



    //注册累加器
    val accumulator = new SessionAccumulator
     sparkSession.sparkContext.register(accumulator)



    //这里不能用map,因为要触发计算
    sessionIdGroupByKey.foreach { case (sessionid,userAction) =>

        var min=0L
        var max= 0L


      userAction.foreach(x=> {

         val time: Long = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(x.action_time).getTime


        if(min!=0){
          min=math.min(time,min)
        }else{
          min=time
        }

        if (max!=0){
         max=math.max(time,max)
        }else{
          max=time
        }})

      val visitlength=max-min
      var visitstep= userAction.size

      // 把满足条件的数据记录的到累加器中
        if(visitlength<=10000){
          accumulator.add("visitLength_10_le")

        }else{
          accumulator.add("visitLength_10_gt")
        }

        if(visitstep<=5){
          accumulator.add("visitStep_5_le")
        }else{
          accumulator.add("visitStep_5_gt")
        }
      accumulator.add("session_count")

    }
    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value
    val visitLength_10_le:Long = sessionCountMap.getOrElse("visitLength_10_le",0)
    val visitLength_10_gt:Long = sessionCountMap.getOrElse("visitLength_10_gt",0)
    val visitStep_5_le:Long = sessionCountMap.getOrElse("visitStep_5_le",0)
    val visitStep_5_gt:Long = sessionCountMap.getOrElse("visitStep_5_gt",0)
    val session_count:Long = sessionCountMap.getOrElse("session_count",0)

    //    5  求占比 =》 符合条件的计数 除以 总数
    val visitLength_10_le_ratio:Double = Math.round( (visitLength_10_le.toDouble/session_count*1000))/10D
    val visitLength_10_gt_ratio:Double = Math.round( (visitLength_10_gt.toDouble/session_count*1000))/10D
    val visitStep_5_le_ratio:Double = Math.round( (visitStep_5_le.toDouble/session_count*1000))/10D
    val visitStep_5_gt_ratio:Double = Math.round( (visitStep_5_gt.toDouble/session_count*1000))/10D


    print(visitLength_10_le_ratio)
    print(visitStep_5_gt_ratio)




    //插入到sql中
    val result: Array[Any] = Array(taskID,jsonstr,session_count,visitLength_10_le_ratio,visitLength_10_gt_ratio,visitStep_5_le_ratio,visitStep_5_gt_ratio)
   //JdbcUtil.executeUpdate("insert into session_stat_info values(?,?,?,?,?,?,?)" ,result)


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
        if (x.search_keyword !=null) searchKeywordList += x.search_keyword
        if (x.search_keyword != -1L) clickProductIdsList += x.click_product_id.toString
        if (x.order_product_ids!=null) orderProductIdsList += x.order_product_ids
        if (x.pay_product_ids!=null) payProductIdsList += x.pay_product_ids
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
    val forgroupBytime: RDD[(String, SessionInfo)] = sessioninfo.map(x=>(x.startTime.split(":")(0),x))

    //按照time分割 (time,Iterable[SessionInfo]))
    val sessioninfoGroupByKey: RDD[(String, Iterable[SessionInfo])] = forgroupBytime.groupByKey()
    
    val Allsize=1000

    val sessioncount: Long = sessionIdGroupByKey.count()
    

      val infoResList = new ListBuffer[SessionInfo]()

    println("==================================")
    //println(sessioninfoGroupByKey.take(1))
    //注意这里要用flatmap,如果用map 返回值就是RDD[ListBuffer[SessionInfo]] ,还需要再取一遍
    val infores2sql: RDD[SessionInfo] = sessioninfoGroupByKey.flatMap { case (time, info) =>

      val currCount: Int = info.size
      val currRat: Long = math.round(currCount.toDouble * Allsize / sessioncount)

      var curset = new mutable.HashSet[SessionInfo]()


      while (curset.size > currRat) {

        val index: Int = new Random().nextInt(curset.size)
        curset += info.toList(index)

      }

      infoResList ++= curset.toList


     // val sessionList: List[SessionInfo] = extractNum(info.toArray,currRat.toInt)
      //sessionList
      infoResList
    }
    println(infores2sql.take(5).mkString(","))


    //5 保存到mysql中
    import sparkSession.implicits._

//    val config2: FileBasedConfiguration = ConfigurationUtil("config.properties").config
//    infores2sql.toDF.write.format("jdbc")
//      .option("url", config2.getString("jdbc.url"))
//      .option("user", config2.getString("jdbc.user"))
//      .option("password", config2.getString("jdbc.password"))
//      .option("dbtable", "random_session_info").mode(SaveMode.Append).save()

  }






  def extractNum[T] ( sourceList: Array[T],num:Int): List[T] ={

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
  
  
  
  
  
  
  
  
  
  
  

  def readUserVisitActionRDD(sparkSession: SparkSession,conditionJsonOBJ:JSONObject):RDD[UserVisitAction]={

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
    import sparkSession.implicits._

  //  println(sparkSession.sql(sql).as[UserVisitAction].rdd.take(1).mkString(""))
    //println(sql)
    sparkSession.sql(sql).as[UserVisitAction].rdd





  }

}
