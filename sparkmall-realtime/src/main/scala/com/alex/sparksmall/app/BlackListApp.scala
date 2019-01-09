package com.alex.sparksmall.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alex.sparkmall.common.utils.RedisUtil
import com.alex.sparksmall.bean.AdsInfo
import org.apache.spark.streaming.dstream.DStream

object BlackListApp {
  def checkUserToBlackList(adsClickInfoDStream: DStream[AdsInfo] ): Unit ={

    //连接1  driver
    // 注意：不能把在driver中建立的连接发送给executor中执行，会报序列化错误
    adsClickInfoDStream.foreachRDD{rdd=>
      //连接2     driver

      rdd.foreachPartition{adsItr=>
        //每个分区 单独执行  executor
        val jedis =RedisUtil.getJedisClient  //在这里建立连接可以节省连接建立次数，节省开销
        for (adsInfo <- adsItr ) {
          //  hincrby( k,f,n) 给某个key的某个field的值增加n
          val daykey: String = new SimpleDateFormat("yyyy-MM-dd").format(new Date(adsInfo.ts))
          //组合成  用户+广告+日期的key
          val countKey=adsInfo.userId+":"+adsInfo.adsId+":"+daykey
          //每次点击进行累加
          jedis.hincrBy("user:ads:dayclick",countKey,1)
          //如果单日累计值达到100，则把用户加入黑名单
          val clickcount: Long = jedis.hget("user:ads:dayclick",countKey).toLong
          if(clickcount>=100){
            jedis.sadd("blacklist",adsInfo.userId)
          }
        }
        jedis.close()

      }


    }

  }

}
