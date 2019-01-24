package com.wanda.ph.dg.test

import java.util.Properties

import com.wanda.ph.dg.utils.Checker

import scala.collection.mutable

object T1 {

  def main(args: Array[String]): Unit = {

//    val map: mutable.HashMap[Int, Int] = mutable.HashMap[Int,Int]()
//
//    map.put(1,1)
    val map2: mutable.HashMap[String, String] = mutable.HashMap[String,String]()

    val t = ("555","555")
    map2.put("3","3")
    map2.put("2","4")

    val props = new Properties
    props.setProperty("1","1")
    props.setProperty("2","2")

    import scala.collection.JavaConversions._
    val ff = map2 ++ props

    println(map2)
  }
}
