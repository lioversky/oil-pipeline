package com.weibo.dip.pipeline.source;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

/**
 * 生成Dstream，stream中只包message
 * Create by hongxun on 2018/7/5
 */
public class StreamingKafka010DataSource extends StreamingDataSource {

  private Map<String, Object> kafkaParams;
  private List<String> topics;

  public StreamingKafka010DataSource(Map params) {
    super(params);
    kafkaParams = (Map<String, Object>) params.get("options");

    String topicStr = (String) kafkaParams.remove("topic");
    topics = Arrays.asList(topicStr.split(","));
  }


  @Override
  public JavaDStream createSource(JavaStreamingContext streamingContext) {
    JavaInputDStream<ConsumerRecord<String, String>> stream =
        KafkaUtils.createDirectStream(
            streamingContext,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topics, kafkaParams)
        );
    return stream.map(record -> record.value());
  }
}
