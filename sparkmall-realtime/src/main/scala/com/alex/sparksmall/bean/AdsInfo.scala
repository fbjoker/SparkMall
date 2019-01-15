package com.alex.sparksmall.bean

import java.text.SimpleDateFormat
import java.util.Date

case class AdsInfo(ts:Long ,area:String ,city:String ,userId:String ,adsId:String ) {


  def  getDayAreaCityAdsIDKey():String={


    val daystr: String = new SimpleDateFormat("yyyy-MM-dd").format( new Date(ts))
    // 天  地区  城市  广告id  拼成一个key
    daystr+":"+area+":"+city+":"+adsId

  }

}
