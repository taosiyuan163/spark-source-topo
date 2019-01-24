package com.wanda.ph.dg.utils

object Checker {

  def get[V](v:Option[V]): V ={

    v match {
      case None =>
        println("错误：值为空")
        System.exit(1)
        v.get
      case _ => v.get
    }
  }
}
