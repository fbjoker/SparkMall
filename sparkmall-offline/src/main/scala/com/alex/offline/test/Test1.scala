package com.alex.offline.test


import scala.collection.mutable


object Test1 {
  def main(args: Array[String]): Unit = {

    val infoResList = new mutable.ListBuffer[Int]()
    var tt = new mutable.HashSet[Int]()

    val aa=List(1,2,3,4)
    tt+=aa(2)
    tt+=aa(3)
    infoResList++=tt.toList
    println(tt.toList)
    println(infoResList)
    println(tt.size)

  }

}
