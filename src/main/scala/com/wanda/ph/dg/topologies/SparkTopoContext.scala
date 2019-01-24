package com.wanda.ph.dg.topologies

import java.io.FileInputStream
import java.util.Properties

import com.wanda.ph.dg.utils.Checker

import scala.collection.mutable

class SparkTopoContext {


  //Todo concurrentHashMap
  private val map: mutable.HashMap[String, String] = mutable.HashMap[String,String]()

  //非自定义loadProp
  def this(path:String) = {
    this()
    loadProp(path)
  }

  def put(k:String,v:String): Unit = map.put(k,v)


  def get(key:String): String = Checker.get(map.get(key))


  def getMap: mutable.HashMap[String, String] = map


  private def loadProp(path: String): Unit ={

    var input:FileInputStream = null
    try{
      val props = new Properties
      input = new FileInputStream(path)
      props.load(input)
      import scala.collection.JavaConversions._
      props.foreach(kv=>{

        map.put(kv._1,kv._2)
      })
    }finally if (input!=null) input.close()

  }
}
