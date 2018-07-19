package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import java.util.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Create by hongxun on 2018/7/5
 */
public class StreamingKafkaDirectDataSource extends StreamingDataSource {

  private Map<String, Object> kafkaParams;
  private List<String> topics;

  public StreamingKafkaDirectDataSource(Map map) {
    super(map);
  }

  @Override
  public JavaDStream createSource(JavaStreamingContext streamingContext) {
    JavaInputDStream<ConsumerRecord<String, String>> stream =
        KafkaUtils.createDirectStream(
            streamingContext,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
    return stream.map(record -> record.value());
  }
}
