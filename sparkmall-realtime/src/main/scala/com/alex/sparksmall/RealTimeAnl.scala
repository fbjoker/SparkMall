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
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
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
    //checkpoint

    val datacheckpoint: DStream[AdsInfo] = inputDs2AdsInfo.checkpoint(Seconds(20))

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

      for ((str,map) <- restop3) {
      //保存数据的格式为时间(天)为key,field为地区,v为对应的广告点击量json串
        //也可以使用hmset直接保存map,需要隐士转换
//       import  collection.JavaConversions._
//        jedisClient.hmset("top3_ads_per_day:"+str,map)
        for ((area,adiscount1) <- map) {
        jedisClient.hset("top3_ads_per_day:"+str,area,adiscount1)
        }
      }

      //关闭连接
      jedisClient.close()

    }




    //需求十
//    最近一小时广告点击量实时统计
//   key last_hour_ads_click   field:    广告id     value:  { 小时分钟:点击次数, 分钟数2：点击次数…..}

//     先转成 Ds[adsid+minute,1L] 然后再Ds[adsid+minute,count]   不能这么做因为map收集的是当前的数据.数据不全
    val adsidAndminuteCount: DStream[(String, Long)] = datacheckpoint.map { adsinfo =>

      //把时间解析成为小时分钟格式
      val clicktimeminute: String = new SimpleDateFormat("HH:mm").format(new Date(adsinfo.ts))

      (adsinfo.adsId +":"+ clicktimeminute, 1L)
      //这里不能自动推断数据类型,必须指定
    }.reduceByKeyAndWindow((a: Long, b: Long) => a + b, Minutes(60), Seconds(20))
    //把Ds[adsid+minute,count] 转换成为 Ds[adsid,iter(minute,count)]
    val adsidandminute1hour: DStream[(String, Iterable[(String, Long)])] = adsidAndminuteCount.map { case (adsidandminute, count) =>

      val adsid: String = adsidandminute.split(":")(0)
      val clicktimeminute: String = adsidandminute.split(":")(1)
      (adsid, (clicktimeminute, count))

    }.groupByKey()

    //整理成为json字符串 Ds[adsid,jsonstr(minute,count)]
    val res2LastHour: DStream[(String, String)] = adsidandminute1hour.map { case (adsid, iter) =>
      val minutejson: String = JsonMethods.compact(JsonMethods.render(iter))


      (adsid, minutejson)

    }



    res2LastHour.foreachRDD { rdd =>

      val jedisClient: Jedis = RedisUtil.getJedisClient
      val res2redis: Array[(String, String)] = rdd.collect()
      for ((adsid, iter) <- res2redis) {

        jedisClient.hset("last_hour_ads_click", adsid, iter)


      }
      jedisClient.close()


    }




/*
    // 1 利用滑动窗口取近一小时数据   rdd[adsInfo]=>window
    val lastHourAdsInfoDstream: DStream[AdsInfo] = inputDs2AdsInfo.window(Minutes(60),Seconds(20))






    // 2 ,按广告进行汇总计数   rdd[ads_hourMinus ,1L]=> reducebykey=>
    val hourMinuCountPerAdsDSream: DStream[(String, Long)] = lastHourAdsInfoDstream.map { adsInfo =>
      val hourMinus: String = new SimpleDateFormat("HH:mm").format(new Date(adsInfo.ts))
      (adsInfo.adsId + "_" + hourMinus, 1L)
    }.reduceByKey(_ + _)
    // 3 把相同广告的 统计汇总     RDD[ads_hourMInus,count]=>
    //    RDD[(adsId, (hourMInus,count)].groubykey
    //    => RDD[(adsId,Iterable[(hourMinu,count)]]
    val hourMinusCountGroupbyAdsDStream: DStream[(String, Iterable[(String, Long)])] = hourMinuCountPerAdsDSream.map { case (adsHourMinusKey, count) =>
      val ads: String = adsHourMinusKey.split("_")(0)
      val hourMinus: String = adsHourMinusKey.split("_")(1)
      (ads, (hourMinus, count))
    }.groupByKey()

    //4 把小时分钟的计数结果转换成json
    //    =>RDD[adsId,Map[hourMinus,count]]
    //    =>Map[adsId,Map[hourMinus,count]]
    //    Map[adsId,hourminusCountJson ]
    val hourMinusCountJsonGroupbyAdsDStream: DStream[(String, String)] = hourMinusCountGroupbyAdsDStream.map { case (ads, hourMinusItr) =>
      val hourMinusCountJson: String = JsonMethods.compact(JsonMethods.render(hourMinusItr))
      (ads, hourMinusCountJson)
    }
    hourMinusCountJsonGroupbyAdsDStream.foreachRDD(rdd=> {

      val hourMinusCountJsonArray: Array[(String, String)] = rdd.collect()

      val jedisClient: Jedis = RedisUtil.getJedisClient
      import collection.JavaConversions._
      jedisClient.hmset("last_hour_ads_click",hourMinusCountJsonArray.toMap)
      jedisClient.close()
    }

    )*/






    ssc.start()
    ssc.awaitTermination()



  }

}
