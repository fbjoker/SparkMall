package com.alex.sparkmall.common.utils

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {

  var jedisPool:JedisPool=null

  def getJedisClient: Jedis = {
    if(jedisPool==null){
      println("开辟一个连接池")
      val config = ConfigurationUtil("config.properties").config
      val host = config.getString("redis.host")
      val port = config.getInt("redis.port")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(1000)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)   //最大空闲
      jedisPoolConfig.setMinIdle(8)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(5000)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试,不测试的话会丢第一次的数据.

      jedisPool=new JedisPool(jedisPoolConfig,"hadoop106",6379)
    }
    //println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    println("获得一个连接")
    jedisPool.getResource
  }

  def main(args: Array[String]): Unit = {

   // val client: Jedis = getJedisClient

    val client = new Jedis("hadoop106",6379)

   val stringToString: util.Map[String, String] = client.hgetAll("ct_user")
    println(stringToString)

  }
}
