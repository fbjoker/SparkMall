package com.alex.sparkmall.mock.utils

import java.util.Random
import scala.collection.mutable._



object RandomNum {

  def apply(fromNum:Int,toNum:Int): Int =  {
    fromNum+ new Random().nextInt(toNum-fromNum+1)
  }
  def multi(fromNum:Int,toNum:Int,amount:Int,delimiter:String,canRepeat:Boolean) ={
    // 实现方法  在fromNum和 toNum之间的 多个数组拼接的字符串 共amount个
    //用delimiter分割  canRepeat为false则不允许重复


    var res1: ArrayBuffer[String] =  new ArrayBuffer[String]()


    if(canRepeat){
      for (x<- 0 until  amount){
        res1+=(fromNum+ new Random().nextInt(toNum-fromNum+1)).toString


      }

    }else{


      for (x<- 0 until  amount){
        var i: Int = fromNum+ new Random().nextInt(toNum-fromNum+1)
        while (res1.contains(i.toString)){
          i=fromNum+ new Random().nextInt(toNum-fromNum+1)
         // println(i)
        }
        res1+=i.toString


      }
    }



    res1.mkString(delimiter)
  }

  def main(args: Array[String]): Unit = {
    val str: String = RandomNum.multi(1, 20, 5, ",", false)

    println(str)
    val tt=Array(20,20,1,18,8)
    println(tt.contains("20"))


  }


}
