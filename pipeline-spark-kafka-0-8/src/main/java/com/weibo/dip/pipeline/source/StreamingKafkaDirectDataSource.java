package com.weibo.dip.pipeline.source;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.listener.OffsetMetricsSources;
import org.apache.spark.streaming.listener.OffsetUpdateListener;
import org.apache.spark.util.KafkaOffsetUtil;
import scala.Tuple2;

/**
 * 0.8.2版本kafka Direct Source
 * Create by hongxun on 2018/7/5
 */
public class StreamingKafkaDirectDataSource extends StreamingDataSource {

  private String topic;

  private Map<String, String> kafkaParams;


  private String consumerGroup;

  private boolean consumerGroupTimeRoll;

  public StreamingKafkaDirectDataSource(Map map) {
    super(map);

    kafkaParams = (Map<String, String>) map.get("parameters");
    topic = kafkaParams.remove("topic");
    consumerGroup = kafkaParams.remove("cosumerGroup");

    if (kafkaParams.get("consumerGroupTimeRoll") != null) {

      consumerGroupTimeRoll = Boolean.parseBoolean(kafkaParams.remove("consumerGroupTimeRoll"));
    } else {
      consumerGroupTimeRoll = false;
    }
  }

  @Override
  public JavaDStream createSource(JavaStreamingContext streamingContext) {
    OffsetUpdateListener listener = new OffsetUpdateListener(consumerGroup);
    if (!consumerGroupTimeRoll) {
      OffsetMetricsSources source = new OffsetMetricsSources();
      listener.registry(source.metricRegistry());
      streamingContext.ssc().sc().env().metricsSystem().registerSource(source);
      streamingContext.addStreamingListener(listener);
      System.out.println("registry listener success!!!");
    }
    //如果静态group,先进行setOrUpdateOffsets,然后获取offset,每次数据都要更新offset
    // 如果为动态,获取最新的offset,不再更新offset
    KafkaOffsetUtil.initKafkaCluster(kafkaParams);
    Set<String> topicsSet = new HashSet<String>(Arrays.asList(topic.split(",", -1)));
    Map<TopicAndPartition, java.lang.Long> offsets = null;
    if (consumerGroupTimeRoll) {
      offsets = KafkaOffsetUtil.getLatestOffsets(topicsSet);
    } else {
//            KafkaOffsetUtil.setOrUpdateOffsets(consumerGroup, topicsSet);
      offsets = KafkaOffsetUtil.setOrUpdateOffsets(consumerGroup, topicsSet);
    }

//        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(streamingContext, String.class,
//                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

    Class<Tuple2<String, String>> c = (Class<Tuple2<String, String>>) (Class) Tuple2.class;
    JavaInputDStream<Tuple2<String, String>> messages = KafkaUtils
        .createDirectStream(streamingContext, String.class,
            String.class, StringDecoder.class, StringDecoder.class, c, kafkaParams, offsets,
            new Function<MessageAndMetadata<String, String>, Tuple2<String, String>>() {
              public Tuple2<String, String> call(MessageAndMetadata<String, String> md)
                  throws Exception {
                return new Tuple2<String, String>(md.key(), md.message());
              }
            }
        );
  //如果不是动态group,更新offset
    if (!consumerGroupTimeRoll) {
      messages.foreachRDD((rdd, time) -> {
        listener.addMessage(time, ((HasOffsetRanges) rdd.rdd()).offsetRanges());
      });
    }

    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }

    });

    return lines;
  }

  @Override
  public void stop() {

  }
}
