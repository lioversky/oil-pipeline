package com.weibo.dip.pipeline.source;

import java.lang.reflect.Constructor;
import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Create by hongxun on 2018/7/5
 */
public class StreamingKafkaDataSourceProvider extends StreamingDataSource {

  private StreamingDataSource kafkaSource;

  public StreamingKafkaDataSourceProvider(Map<String, Object> map) {
    super(map);
    try {
      String className = "com.weibo.dip.pipeline.source.StreamingKafkaReceiverSource";
      Constructor<StreamingDataSource> constructor = (Constructor<StreamingDataSource>) Class
          .forName(className).getConstructor(Map.class);
      this.kafkaSource = constructor.newInstance(map);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public JavaDStream createSource(JavaStreamingContext streamingContext) {

    return kafkaSource.createSource(streamingContext);
  }
}
