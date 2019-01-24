package com.wanda.ph.dg

import java.io.{FileInputStream, IOException}
import java.util.Properties

import com.wanda.ph.dg.constants.Constants
import com.wanda.ph.dg.kafka.KafkaHelper
import com.wanda.ph.dg.topologies.{SparkTopoContext, SparkTopology}
import com.wanda.ph.dg.utils.Checker
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object MainSparkStreaming {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("Spark-Streaming")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val session: SparkSession = SparkSession.builder().getOrCreate()
    val ssc = new StreamingContext(sc, Seconds(20))

    val stc = new SparkTopoContext("path")


    val topoName = stc.get(Constants.CLASS_NAME)

    val sparkTopo = Class.forName(topoName).newInstance.asInstanceOf[SparkTopology]

    sparkTopo.config(stc)

    val brokerList = stc.get(Constants.BROKERS)

    val topicList = stc.get(Constants.TOPICS)


    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList)
//
//
    val messagesDStream = KafkaHelper.loadTopicAndMessageFromKafka(ssc, topicList, kafkaParams)

    sparkTopo.process(messagesDStream)

    ssc.start()
    ssc.awaitTermination()
  }


}
