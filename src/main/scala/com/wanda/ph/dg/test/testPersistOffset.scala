package com.wanda.ph.dg.test

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

object testPersistOffset {

  def main(args: Array[String]): Unit = {

    val ZK_HOSTS: String = "192.168.9.32:2181,192.168.9.31:2181,192.168.9.30:2181"

    val conf = new SparkConf().setAppName("Spark-Streaming")

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    val session: SparkSession = SparkSession.builder().getOrCreate()
    val ssc = new StreamingContext(sc, Seconds(10))


    val kafkaParam = Map[String, String]("metadata.broker.list" -> "192.168.9.30:9092,192.168.9.31:9092,192.168.9.32:9092")

    val TOPIC_ORACL_FCS_ACCT_REGISTER: String = "aa"

    val TOPIC_ORACL_DS_OPERATION: String = "bb"

    val TOPIC_ORACL_DS_USER: String = "test-kudu-ten_day_ds_user"

    val TOPIC_ORACL_LM_INSTALLMENT_TRAN: String = "test-kudu-ten_day_tran"

    val TOPIC_ORACL_LM_INSTALLMENT_TRAN_D: String = "test-kudu-ten_day_tran_d"

    val TOPIC_ORACL_DS_IDENTITY_CARD_FILE: String = "test-kudu-ten_day_ds_icf"

    val TOPIC_ORACL_REQ: String = "test-kudu-ten_day_req"

    val topic: String = s"$TOPIC_ORACL_FCS_ACCT_REGISTER,$TOPIC_ORACL_DS_OPERATION" //消费的 topic 名字
    val topics: Set[String] = Set(topic) //创建 stream 时使用的 topic 名字集合

    val topicDirs = new ZKGroupTopicDirs("test_spark_streaming_group", topic) //创建一个 ZKGroupTopicDirs 对象，对保存
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}" // 获取 zookeeper 中的路径，这里会变成 /consumers/test_spark_streaming_group/offsets/topic_name

    val zkClient = new ZkClient(ZK_HOSTS) //zookeeper 的host 和 ip，创建一个 client
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）

    var kafkaStream: InputDStream[(String, String)] = null
    var fromOffsets: Map[TopicAndPartition, Long] = Map() //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    if (children > 0) { //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong) //将不同 partition 对应的 offset 增加到 fromOffsets 中
        println("@@@@@@ topic[" + topic + "] partition[" + i + "] offset[" + partitionOffset + "] @@@@@@")
      }

      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message()) //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)
    }
    else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topics) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
    }

    var offsetRanges = Array[OffsetRange]()
    kafkaStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(msg => msg._2).foreachRDD { rdd =>
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString) //将该 partition 的 offset 保存到 zookeeper
        println(s"@@@@@@ topic  ${o.topic}  partition ${o.partition}  fromoffset ${o.fromOffset}  untiloffset ${o.untilOffset} #######")
      }

      rdd.foreachPartition(
        message => {
          while (message.hasNext) {
            println(s"@^_^@   [" + message.next() + "] @^_^@")
          }
        }
      )
    }
  }
}
