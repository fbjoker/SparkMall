package com.alex.offline.app

import java.text.SimpleDateFormat

import com.alex.offline.utils.SessionAccumulator
import com.alex.sparkmall.common.bean.UserVisitAction
import com.alibaba.fastjson.JSONObject
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object SessionStatApp {
   def statsession(sparkSession: SparkSession, taskID: String, jsonstr: String, sessionIdGroupByKey: RDD[(String, Iterable[UserVisitAction])]): Unit = {
    //需求一
    //统计出符合筛选条件的session中，访问时长在小于10s含、10s以上各个范围内的session数量占比。访问步长在小于等于5，和大于5次的占比
    //1 根据过滤条件 取出符合的日志RDD集合 成为RDD[UserVisitAction],使用hql完成(小难点)
    // 2 以sessionId为key 进行聚合 =》 RDD[sessionId,Iterable[UserVisitAction]]
    // 3 把每个iterable 遍历 取最大时间和最小时间 ，取差 ，得session时长
    // 4 根据条件进行计数 利用累加器进行计数, 自定义累加器(小难点)
    // 5 求占比 =》 符合条件的计数 除以 总数
    // 6 结果保存到mysql

    //注册累加器
    val accumulator = new SessionAccumulator
    sparkSession.sparkContext.register(accumulator)



    //这里不能用map,因为要触发计算
    sessionIdGroupByKey.foreach { case (sessionid, userAction) =>

      var min = 0L
      var max = 0L


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
      })

      val visitlength = max - min
      var visitstep = userAction.size

      // 把满足条件的数据记录的到累加器中
      if (visitlength <= 10000) {
        accumulator.add("visitLength_10_le")

      } else {
        accumulator.add("visitLength_10_gt")
      }

      if (visitstep <= 5) {
        accumulator.add("visitStep_5_le")
      } else {
        accumulator.add("visitStep_5_gt")
      }
      accumulator.add("session_count")

    }
    val sessionCountMap: mutable.HashMap[String, Long] = accumulator.value
    val visitLength_10_le: Long = sessionCountMap.getOrElse("visitLength_10_le", 0)
    val visitLength_10_gt: Long = sessionCountMap.getOrElse("visitLength_10_gt", 0)
    val visitStep_5_le: Long = sessionCountMap.getOrElse("visitStep_5_le", 0)
    val visitStep_5_gt: Long = sessionCountMap.getOrElse("visitStep_5_gt", 0)
    val session_count: Long = sessionCountMap.getOrElse("session_count", 0)

    //    5  求占比 =》 符合条件的计数 除以 总数
    val visitLength_10_le_ratio: Double = Math.round((visitLength_10_le.toDouble / session_count * 1000)) / 10D
    val visitLength_10_gt_ratio: Double = Math.round((visitLength_10_gt.toDouble / session_count * 1000)) / 10D
    val visitStep_5_le_ratio: Double = Math.round((visitStep_5_le.toDouble / session_count * 1000)) / 10D
    val visitStep_5_gt_ratio: Double = Math.round((visitStep_5_gt.toDouble / session_count * 1000)) / 10D


    println(visitLength_10_le_ratio)
    println(visitLength_10_gt_ratio)
    println(visitStep_5_gt_ratio)
    println(visitStep_5_le_ratio)


    //插入到sql中
    val result: Array[Any] = Array(taskID, jsonstr, session_count, visitLength_10_le_ratio, visitLength_10_gt_ratio, visitStep_5_le_ratio, visitStep_5_gt_ratio)
    //JdbcUtil.executeUpdate("insert into session_stat_info values(?,?,?,?,?,?,?)" ,result)
  }


}
