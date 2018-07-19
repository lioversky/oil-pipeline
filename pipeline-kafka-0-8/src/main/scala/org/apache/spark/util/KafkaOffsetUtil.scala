package org.apache.spark.util

import kafka.common.TopicAndPartition
import org.apache.spark.SparkException
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}

import scala.collection.JavaConversions._

/**
  * Created by hongxun on 16/11/15.
  */
object KafkaOffsetUtil {
  private var kc: KafkaCluster = null;

  def initKafkaCluster(kafkaParams: java.util.Map[String, String]): Unit = {
    this.synchronized {
      if (kc == null) {
        kc = new KafkaCluster(kafkaParams.toMap)
      }
    }
  }

  /**
    * 更新zookeeper上的消费offsets
    *
    * @param offsetsList
    * @param groupId
    */
  def updateZKOffsets(offsetsList: Array[OffsetRange], groupId: String): Unit = {
    //    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

    val offsets = offsetsList.map(offsets =>
      (TopicAndPartition(offsets.topic, offsets.partition), offsets.untilOffset)
    )
    val o = kc.setConsumerOffsets(groupId, offsets.toMap)
    if (o.isLeft) {
      println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
    }
  }

  def getZKOffsets(groupId: String, topics: java.util.Set[String]): java.util.Map[TopicAndPartition, java.lang.Long] = {
    val partitionsE = kc.getPartitions(topics.toSet)
    if (partitionsE.isLeft)
      throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
    val partitions = partitionsE.right.get
    val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
    if (consumerOffsetsE.isLeft)
      throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
    val offsets = consumerOffsetsE.right.get
    offsets.map({ case (p, l) => (p, java.lang.Long.valueOf(l)) })
  }

  def getLatestOffsets(topics: java.util.Set[String]): java.util.Map[TopicAndPartition, java.lang.Long] = {
    val partitionsE = kc.getPartitions(topics.toSet)
    if (partitionsE.isLeft)
      throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
    val kafkaPartitions = partitionsE.right.get

    val leaderLatestOffsets = kc.getLatestLeaderOffsets(kafkaPartitions).right.get
    leaderLatestOffsets.map {
      case (tp, offset) => (tp, java.lang.Long.valueOf(offset.offset))
    }
  }

  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    *
    * @param topics
    * @param groupId
    */
  def setOrUpdateOffsets(groupId: String, topics: java.util.Set[String]): java.util.Map[TopicAndPartition, java.lang.Long] = {
    //    topics.foreach(topic => {

    println("current topic:" + topics)
    var hasConsumed = true
    val kafkaPartitionsE = kc.getPartitions(topics.toSet)
    if (kafkaPartitionsE.isLeft) throw new SparkException("get kafka partition failed:")
    val kafkaPartitions = kafkaPartitionsE.right.get
    val consumerOffsetsE = kc.getConsumerOffsets(groupId, kafkaPartitions)
    if (consumerOffsetsE.isLeft) hasConsumed = false
    val latestOffsetsE = kc.getLatestLeaderOffsets(kafkaPartitions)
    if (latestOffsetsE.isLeft)
      throw new SparkException(s"get kafka partition failed: ${latestOffsetsE.left.get}")
    val leaderLatestOffsets = latestOffsetsE.right.get
    if (hasConsumed) {
      //如果有消费过，有两种可能，如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
      //针对这种情况，只要判断一下zk上的consumerOffsets和leaderEarliestOffsets的大小，如果consumerOffsets比leaderEarliestOffsets还小的话，说明是过时的offsets,这时把leaderEarliestOffsets更新为consumerOffsets
      val leaderEarliestOffsets = kc.getEarliestLeaderOffsets(kafkaPartitions).right.get
      //        println(leaderEarliestOffsets)
      val consumerOffsets = consumerOffsetsE.right.get
      val flag = consumerOffsets.forall {
        case (tp, n) => n > leaderEarliestOffsets(tp).offset
      } && consumerOffsets.forall {
        case (tp, n) => n < leaderLatestOffsets(tp).offset
      }
      //如果consumerOffsets全部大于leaderEarliestOffsets，不处理
      if (flag) {
        println("consumer group:" + groupId + " offsets正常，无需更新")
        println(consumerOffsets)
        consumerOffsets.map {
          case (tp, offset) => (tp, java.lang.Long.valueOf(offset))
        }
      }
      else {
        //否则取最大值重置offset
        println("consumer group:" + groupId + " offsets已经过时，更新为leaderEarliestOffsets")
        println(consumerOffsets)
        //        println(leaderEarliestOffsets)
        val offsets = consumerOffsets.map {
          case (tp, n) => (tp, Math.min(Math.max(n, leaderEarliestOffsets(tp).offset), leaderLatestOffsets(tp).offset))
        }
        kc.setConsumerOffsets(groupId, offsets)
        offsets.map {
          case (tp, offset) => (tp, java.lang.Long.valueOf(offset))
        }
      }
    }
    else {
      //如果没有被消费过，则从最新的offset开始消费。

      println(leaderLatestOffsets)
      println("consumer group:" + groupId + " 还未消费过，更新为leaderLatestOffsets")
      val offsets = leaderLatestOffsets.map {
        case (tp, offset) => (tp, offset.offset)
      }
      kc.setConsumerOffsets(groupId, offsets)
      leaderLatestOffsets.map {
        case (tp, offset) => (tp, java.lang.Long.valueOf(offset.offset))
      }
    }
    //    })
  }

  def main(args: Array[String]): Unit = {
    val map: Map[String, String] = Map("metadata.broker.list" -> "first.kafka.dip.weibo.com:9092,second.kafka.dip.weibo.com:9092,third.kafka.dip.weibo.com:9092,fourth.kafka.dip.weibo.com:9092,fifth.kafka.dip.weibo.com:9092",
      "zookeeper.connect" -> "first.zookeeper.dip.weibo.com:2181,second.zookeeper.dip.weibo.com:2181,third.zookeeper.dip.weibo.com:2181/kafka/k1001")
    initKafkaCluster(map)

    //    val consumer = new SimpleConsumer("second.kafka.dip.weibo.com",9092,5000,10000000,"12345564")
    var topics: Set[String] = Set("app_weibomobilekafka1234_wwwlog")
    //    val offsets = KafkaOffsetUtil.setOrUpdateOffsets("mobile-distribute-2", topics)

    val kafkaPartitionsE = kc.getPartitions(topics)
    if (kafkaPartitionsE.isLeft) throw new SparkException("get kafka partition failed:")
    val kafkaPartitions = kafkaPartitionsE.right.get

    val leaderLatestOffsets = kc.getLatestLeaderOffsets(kafkaPartitions).right.get
    //    val leaderEarliestOffsets = kc.getEarliestLeaderOffsets(kafkaPartitions).right.get

//    val offsets = leaderLatestOffsets.map {
//      case (tp, offset) => (tp, offset.offset)
//    }
//    kc.setConsumerOffsets("urlargs_app_online", offsets)
//

    val consumerOffsetsE = kc.getConsumerOffsets("urlargs_app_online", kafkaPartitions)
    if (consumerOffsetsE.isLeft) throw new SparkException("don't have consumer")
    val consumerOffsets = consumerOffsetsE.right.get

    consumerOffsets.foreach {
      case (tp, n) => {
        val offset = leaderLatestOffsets(tp).offset
        val last = offset - n
        println(s"$tp,offset=$offset,consumer=$n,$last")
        //        if (last  > 100000) {
        //          val host = kc.findLeader(tp.topic, tp.partition).right.get._1
      }
    }

        println(consumerOffsets.map{case (tp, n) =>leaderLatestOffsets(tp).offset - n}.sum)
  }
}