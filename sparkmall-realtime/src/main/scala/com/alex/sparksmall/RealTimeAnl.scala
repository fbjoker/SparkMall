package com.alex.sparksmall

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alex.sparkmall.common.utils.{MyKafkaUtil, RedisUtil}
import com.alex.sparksmall.app.BlackListApp
import com.alex.sparksmall.bean.AdsInfo
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._
import org.json4s.native

import scala.collection.immutable

object RealTimeAnl {
  def main(args: Array[String]): Unit = {

    //sc
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("realtimeADS")

    val sc = new SparkContext(sparkConf)
    //ssc

    val ssc = new StreamingContext(sc,Seconds(4))


      val topic="ads_log1"

       val kafkaInputDs: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic,ssc)


    //把输入的数据转成对象,方便后续的操作
    val inputDs2AdsInfo: DStream[AdsInfo] = kafkaInputDs.map { record =>
      val adsinfo: Array[String] = record.value().split(" ")

      AdsInfo(adsinfo(0).toLong, adsinfo(1), adsinfo(2), adsinfo(3), adsinfo(4))
    }



    println("x1")
 //   BlackListApp.checkUserToBlackList(inputDs2AdsInfo)

    //需求七

    //过滤流,  把黑名单的数据过滤掉,返回的还是一个流,

    val filterDS: DStream[AdsInfo] = inputDs2AdsInfo.transform { rdd =>
      val conn = RedisUtil.getJedisClient
      val blacklist: util.Set[String] = conn.smembers("blacklist")

      //广播变量必须放到driver中执行,这样才会周期的执行,取得最新的量,不然就加载的时候取一次,
      val blacklistbc: Broadcast[util.Set[String]] = sc.broadcast(blacklist)

      //判断是否在黑名单中,
      val filteradsinfo: RDD[AdsInfo] = rdd.filter { adsinfo =>

        !blacklistbc.value.contains(adsinfo.userId)
      }
      conn.close()
      filteradsinfo


    }


    //添加到黑名单

    //每一个时间间隔看做是是一个大的rdd
    filterDS.foreachRDD{ rdd=>

      //driver

      //分区遍历每个rdd内的数据,每个分区内创建连接,减少开销
      rdd.foreachPartition{adsinfo=>
       // val conn = RedisUtil.getJedisClient
        val conn = new Jedis("hadoop106",6379)

       // println("x2")

        for (elem <- adsinfo) {
        //拼接字段,每天\广告ID\用户点击的格式


          val day: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(elem.ts))
          //统计key为每天\广告ID\用户点击的数据, 保存到redis里,使用hash
          val key: String = day+elem.adsId+elem.userId
          //累加
          conn.hincrBy("user:ads:dayclick",key,1)
          val count: Long = conn.hget("user:ads:dayclick",key).toLong

          //如果统计的单日点击量到达100,加入黑名单
          if(count>100){
            //黑名单表使用set, 是把用户id加入黑名单
            conn.sadd("blacklist",elem.userId)
          }
        }
        conn.close()
      }
    }



    //需求八
    //广告的点击量的实时统计
    //把数据整理成为// 天  地区  城市 广告id  拼成一个key,然后统计
    val dayANDareaAndcityadsCount: DStream[(String, Long)] = filterDS.map{adsinfo=> (adsinfo.getDayAreaCityAdsIDKey(),1L)}.reduceByKey(_+_)

    //使用有状态的操作, currCount是当前的结果, totalCount是总的结果
    val adsCount: DStream[(String, Long)] = dayANDareaAndcityadsCount.updateStateByKey { (currCount: Seq[Long], totalCount: Option[Long]) =>
      //把当前结果和总的结果相加
      val newtotalCount: Long = currCount.sum + totalCount.getOrElse(0L)

      Some(newtotalCount)
    }



    //updateStateByKey需要设置缓存目录,
    sc.setCheckpointDir("./checkPoint")
    
    //把结果存到redis里面,除了使用transform里用连接池,还可以使用本地化到driver,前提是数据量小

    adsCount.foreachRDD{rdd=>
      
       val adsCount: Array[(String, Long)] = rdd.collect()

      val conn = RedisUtil.getJedisClient
      for (elem <- adsCount) {

        //把数据保存到redis的hash里,
      conn.hset("day:area:city:adsCount",elem._1,elem._2.toString)
        //不使用updateStateByKey,而直接用redis来统计数据的方法. 拼成一个key,然后统计(key,count)的结果传到redis里加
        //conn.hincrBy("day:area:city:adsCount",key,count )


      }
      conn.close()
    }




    //需求九
    //每天各地区 top3 热门广告
    //最终的结果为top3_ads_per_day:2018-11-26,   field:   华北      value:  {“2”:1200, “9”:1100, “13”:910}
    //   field: 东北    value:{“11”:1900, “17”:1820, “5”:1700}
    //保存在 redis保存为一条hash结构 的kv



    //需求八返回的结果是 天  地区  城市 广告id  ,需要整理为 天  地区   广告id
    val dayAreaAdsid: DStream[(String, Long)] = adsCount.map { case (dayareacityads, count) =>
      val infosplit: Array[String] = dayareacityads.split(":")
      val day: String = infosplit(0)
      val area: String = infosplit(2)
      val adsid: String = infosplit(3)

      //整理数据格式为 天  地区   广告id,切分掉城市了, 可能出相同的天地区广告,需要再聚合
      (day + ":" + area + ":" + adsid, count)
    }.reduceByKey(_ + _)

    //按照(天,(地区,(广告id,count))) 按照天进行聚合

    val dayAndareaadsidCount: DStream[(String, Iterable[(String, (String, Long))])] = dayAreaAdsid.map { case (dayareaadsid, count) =>

      val infosplit: Array[String] = dayareaadsid.split(":")
      val day: String = infosplit(0)
      val area: String = infosplit(1)
      val adsid: String = infosplit(2)


      (day, (area, (adsid, count)))
    }.groupByKey()
    //把Iterable[(地区,(广告id,count))]  拆分为[地区,Iterable(广告id,count)] ,按照地区来进行聚合
    val dayAndareatop3: DStream[(String, Map[String, String])] = dayAndareaadsidCount.map { case (day, areaAdsidItr) =>
      //得到的是[地区,Iterable[(地区,(广告id,count))]]
      val areaAndadisidCount: Map[String, Iterable[(String, (String, Long))]] = areaAdsidItr.groupBy { case (area, adsidItr) => area }

      //[地区,Iterable[(地区,(广告id,count))]] 把里面的迭代对象转换为Iterable[(广告id,count)]
      val adisAndCountList: Map[String, String] = areaAndadisidCount.map { case (area, areaAdisCountItr) =>
        val areaAndadiscount: Iterable[(String, Long)] = areaAdisCountItr.map { case (area, adidcount) =>
          adidcount
        }
        //按照count排序,并选择前3个
        val sortedCount: List[(String, Long)] = areaAndadiscount.toList.sortWith(_._2 > _._2).take(3)

        //把sortcount转换为json字符串 ,需要导包
        //import org.json4s.native.JsonMethods
        //import org.json4s.JsonDSL._
        // fastjson gson jackson 面向java    //json4s  面向scala
        val jsonstr: String = JsonMethods.compact(JsonMethods.render(sortedCount))


        (area, jsonstr)
      }
      adisAndCountList

      (day, adisAndCountList)


    }


    //保存数据到redis
    dayAndareatop3.foreachRDD{rdd=>


      val jedisClient: Jedis = RedisUtil.getJedisClient
     val restop3: Array[(String, Map[String, String])] = rdd.collect()
      for (elem <- restop3) {





      }




    }






    ssc.start()
    ssc.awaitTermination()



  }

}
