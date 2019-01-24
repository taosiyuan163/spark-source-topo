package com.wanda.ph.dg.topologies

import org.apache.spark.streaming.dstream.InputDStream

trait SparkTopology {

  def process (DStream:InputDStream[(String, String)])

  def config (context:SparkTopoContext)
}
