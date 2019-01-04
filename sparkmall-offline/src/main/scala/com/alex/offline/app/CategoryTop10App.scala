package com.alex.offline.app

import com.alex.offline.bean.Categoryinfo
import com.alex.offline.utils.CategoryAccumulator
import com.alex.sparkmall.common.bean.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.{immutable, mutable}

object CategoryTop10App {

   def statCategoryTop10(sparkSession: SparkSession, filterRDD: RDD[UserVisitAction],taskID:String): List[Categoryinfo] = {
    //需求三

    //注册累加器

    val categroyaccumulator = new CategoryAccumulator

    sparkSession.sparkContext.register(categroyaccumulator)



    //把商品按照点击\订单\支付进行累加
    filterRDD.foreach { useraction =>

      if (useraction.click_category_id != -1) {
        categroyaccumulator.add(useraction.click_category_id + "_click")

      } else if (useraction.order_category_ids != null) {
        //订单类别是 3,5,7这样的,需要拆分为单个的cid
        useraction.order_category_ids.split(",").foreach(x =>
          categroyaccumulator.add(x + "_order")
        )

      } else if (useraction.pay_category_ids != null) {

        useraction.pay_category_ids.split(",").foreach(x =>
          categroyaccumulator.add(x + "_pay")
        )


      }


    }

    //取出累加的结果
    val categoryCountMap: mutable.HashMap[String, Long] = categroyaccumulator.value

    // categoryCountMap.foreach(println)
    //按照cid进行分组 之前的格式((15_order,301),(15_pay,301),(15_click,1740))  分组后 (15,(((15_order,301),(15_pay,301),(15_click,1740))))
    val categorygroupby: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy { case (cidAction, count) => cidAction.split("_")(0) }

    val categoryinfos: immutable.Iterable[Categoryinfo] = categorygroupby.map { case (cid, categorycountmap) =>
      val click: Long = categorycountmap.getOrElse(cid + "_click", 0L)
      val order: Long = categorycountmap.getOrElse(cid + "_order", 0L)
      val pay: Long = categorycountmap.getOrElse(cid + "_pay", 0L)

      Categoryinfo(taskID, cid, click, order, pay)
    }
    val sortedCategoryinfo: List[Categoryinfo] = categoryinfos.toList.sortWith { case (a, b) =>

      if (a.clickCount < b.clickCount) {

        false
      } else if (a.clickCount == b.clickCount) {
        if (a.orderCount < b.orderCount) {

          false
        } else if (a.orderCount == b.orderCount) {
          if (a.payCount < b.payCount) {
            false
          } else {
            true
          }
        } else {
          true
        }

      } else {
        true
      }


    }.take(10)
    val toSqllist: List[Array[Any]] = sortedCategoryinfo.map(item =>
      Array(item.taskId, item.categoryId, item.clickCount, item.orderCount, item.payCount))
    //写入数据库
    //toSqllist.foreach(x=>println(x.mkString("\t")))
    // JdbcUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)",toSqllist)
    sortedCategoryinfo
  }
}
