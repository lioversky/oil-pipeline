package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


/**
 * Create by hongxun on 2018/7/5
 */
class StreamingHdfsTimeDataSource extends StreamingDataSource {

  public StreamingHdfsTimeDataSource(Map map) {
    super(map);
  }

  @Override
  public JavaDStream createSource(JavaStreamingContext streamingContext) {
    return null;
  }
}

/**
 * Create by hongxun on 2018/7/5
 */
class StreamingKafkaDataSource extends StreamingDataSource {

  private StreamingDataSource kafkaSource;
  private StreamingKafkaSourceProvider streamingKafkaSourceProvider = StreamingKafkaSourceProvider.newInstance();

  public StreamingKafkaDataSource(Map<String, Object> map) {
    super(map);

    try {
      kafkaSource = streamingKafkaSourceProvider.createDataSource(map);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public JavaDStream createSource(JavaStreamingContext streamingContext) {

    return kafkaSource.createSource(streamingContext);
  }
}