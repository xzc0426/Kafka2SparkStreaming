package com.alphasta.SparkStreaming

import com.alphasta.kafka.KafkaConnPool
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo, TopicMetadataRequest}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.streaming.EsSparkStreaming

/**
  * Created by Xu on 2019/7/11.
  */
object Streaming {
  def main(args: Array[String]): Unit = {
    //初始化
    val conf = new SparkConf().setAppName("Kafka").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))

    //创建连接Kafka参数

    val brokerList = "master01:9092,slave01:9092,slave02:9092"
    val zookeeper = "master01:2181,slave01:2181,slave02:2181"
    val sourceTopic = "source101"
    val targetTopic = "target101"
    val groupId = "consumer101"

    //创建Kafka的连接参数
    val kafkaParam = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
    )

    //连接Kafka
    //不是第一次创建则从zk中恢复
    var textKafkaDStream: InputDStream[(String, String)] = null
    textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(sourceTopic))

    // 创建保存topicDir
    val topicDirs = new ZKGroupTopicDirs(groupId, sourceTopic)

    //创建topic对应offsetDir
    val zkTopicPath = s"${topicDirs.consumerOffsetDir}"

    //连接zk
    val zkClient = new ZkClient(zookeeper)
    val children = zkClient.countChildren(zkTopicPath)

    //判断是否有offset
    if (children > 0) {

      //获取上次offset位置
      var fromOffsets: Map[TopicAndPartition, Long] = Map()

      //获取Kafka集群元信息
      val topicList = List(sourceTopic)

      //创建连接
      val getLeaderConsumer = new SimpleConsumer("master01", 9092, 100000, 10000, "offsetLookUp")

      //创建获取元信息的request
      val request = new TopicMetadataRequest(topicList, 0)

      //获取返回元信息
      val response = getLeaderConsumer.send(request)

      //解析元信息
      val topicsMetadataOption = response.topicsMetadata.headOption

      //[partitionID,对应的主节点]
      val partitions = topicsMetadataOption match {
        case Some(tm) => tm.partitionsMetadata.map(x => (x.partitionId, x.leader.get.host)).toMap[Int, String]
        case None => Map[Int, String]()
      }
      getLeaderConsumer.close()

      println("partitions: " + partitions)
      println("offset数量是: " + children)

      //获取每个分区最小offset
      for (i <- 0 until children) {

        //获取zk中offset
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        println(s"获取到partition ${i} 的offset是： ${partitionOffset}")

        //获取第i个分区最小offset
        //创建到第i个分区主分区的host连接
        val consumerMin = new SimpleConsumer(partitions(i), 9092, 100000, 10000, "getMinOffset")

        //获取partition的offset
        val tp = TopicAndPartition(sourceTopic, i)

        //创建一个请求
        val requestMin = OffsetRequest(Map(tp -> PartitionOffsetRequestInfo(OffsetRequest.EarliestTime, 1)))

        //获取最小offset
        val curOffsets = consumerMin.getOffsetsBefore(requestMin).partitionErrorAndOffsets(tp).offsets

        consumerMin.close()

        //校准offset
        var nextOffset = partitionOffset.toLong
        if (curOffsets.length > 0 && curOffsets.head > nextOffset) {
          nextOffset = curOffsets.head
        }
        println(s"Partition ${i} 修正后的offset是：${nextOffset}")
        fromOffsets += (tp -> nextOffset)
      }

      zkClient.close()

      println("从zookeeper中恢复创建")
      //从zk中恢复创建，将fromOffsets传入
      //messageHandler: MessageAndMetadata[K, V] => R
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
      textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParam, fromOffsets, messageHandler)

    } else {
      //直接创建
      println("直接创建")
      textKafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, Set(sourceTopic))
    }

    //用RDD操作DStream
    val offsetRanges = Array[OffsetRange]()
    val textKafkaDStream2 = textKafkaDStream.transform { rdd =>
      rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }

    //业务处理
    textKafkaDStream2.map(s => "key:" + s._1 + " value:" + s._2).foreachRDD { rdd =>
      rdd.foreachPartition { items =>

        //需要用到连接池
        //创建到Kafka的连接
        val pool = KafkaConnPool(brokerList)
        //拿到连接
        val kafkaProxy = pool.borrowObject()
        //插入数据
        for (item <- items)
          kafkaProxy.send(targetTopic, item)

        //将结果保存到ES
        //EsSparkStreaming.saveJsonToEs(textKafkaDStream2, "2")

        //关闭连接
        pool.returnObject(kafkaProxy)
      }

      //将Kafka每个partition的offset更新到zk
      val updateTopicDirs = new ZKGroupTopicDirs(groupId, sourceTopic)
      val updateZkClient = new ZkClient(zookeeper)

      for (offset <- offsetRanges) {
        println(s"partition ${offset.partition} 保存到zk中的offset是: ${offset.fromOffset.toString}")
        val zkPath = s"${updateTopicDirs.consumerOffsetDir}/${offset.partition}"
        ZkUtils.updatePersistentPath(updateZkClient, zkPath, offset.fromOffset.toString)
      }
      updateZkClient.close()

    }
    //启动程序
    ssc.start()
    //用于捕获异常
    ssc.awaitTermination()
  }
}
