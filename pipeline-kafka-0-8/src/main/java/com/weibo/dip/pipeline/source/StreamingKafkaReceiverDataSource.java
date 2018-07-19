package com.weibo.dip.pipeline.source;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.joda.time.DateTime;
import scala.Tuple2;

/**
 * Create by hongxun on 2018/7/5
 */
public class StreamingKafkaReceiverDataSource extends StreamingDataSource {

  private String zookeeper;

  private String cosumerGroup;

  private String topics;

  private int threadPerStream;

  private int numStreams;

  private String storageLevel;

  private boolean consumerGroupTimeRoll;

  private Map<String, Integer> topicMap;


  public StreamingKafkaReceiverDataSource(Map map) {
    super(map);

    Map<String, Object> parameters = (Map<String, Object>) map.get("parameters");

    zookeeper = (String) parameters.get("zookeeper");

    cosumerGroup = (String) parameters.get("cosumerGroup");

    topics = (String) parameters.get("topic");

    threadPerStream = ((Number) parameters.get("threadPerStream")).intValue();

    numStreams = ((Number) map.get("numStreams")).intValue();

    storageLevel = (String) map.get("storageLevel");

    if (parameters.get("consumerGroupTimeRoll") != null) {

      consumerGroupTimeRoll = (Boolean) parameters.get("consumerGroupTimeRoll");
    } else {
      consumerGroupTimeRoll = false;
    }

    topicMap = Maps.newHashMap();

    String[] topicArrays = topics.split(",", -1);

    for (String topic : topicArrays) {
      topicMap.put(topic, threadPerStream);
    }

  }

  @Override
  public JavaDStream createSource(JavaStreamingContext streamingContext) {
    JavaDStream<String> dStream = null;

    List<JavaPairDStream<String, String>> kafkaStreams = new ArrayList<JavaPairDStream<String, String>>(
        numStreams);

    String realConsumerGroup = cosumerGroup;

    if (consumerGroupTimeRoll) {
      realConsumerGroup =
          cosumerGroup + DateTime.now().toString("yyyyMMddHHmmss");
    }

    for (int i = 0; i < numStreams; i++) {

      kafkaStreams
          .add(KafkaUtils.createStream(streamingContext, zookeeper, realConsumerGroup, topicMap,
              StorageLevel.fromString(storageLevel)));
    }
    dStream = streamingContext
        .union(kafkaStreams.get(0), kafkaStreams.subList(1, kafkaStreams.size()))
        .map(new Function<Tuple2<String, String>, String>() {
          public String call(Tuple2<String, String> arg0) throws Exception {
            return arg0._2();
          }
        });

    return dStream;
  }
}
