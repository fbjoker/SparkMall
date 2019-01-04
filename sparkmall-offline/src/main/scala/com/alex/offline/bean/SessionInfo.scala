package com.alex.offline.bean

case class SessionInfo(taskId:String ,
                       sessionId:String ,
                       startTime:String ,
                       stepLength:Int,
                       visitLength:Long,
                       searchKeywords :String ,
                       clickProductIds:String,
                       orderProductIds:String,
                       payProductIds:String
                      ) {

}
