package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


/**
 * 按batch time读取对应时间段的文件，生成DStream
 * Create by hongxun on 2018/7/5
 */
class StreamingHdfsTimeDataSource extends StreamingDataSource {

  // todo: HdfsTime Dstream
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
class StreamingKafkaSourceDelegate extends StreamingDataSource {

  /**
   * 实际的对应版本的kafka Source
   */
  private StreamingDataSource kafkaSource;
  /**
   * kafka source生成器，当未加载到Provider时返回空
   */
  private StreamingKafkaSourceProvider streamingKafkaSourceProvider = StreamingKafkaSourceProvider
      .newInstance();

  public StreamingKafkaSourceDelegate(Map<String, Object> map) {
    super(map);

    try {
      kafkaSource = streamingKafkaSourceProvider.createDataSource(map);
    } catch (Exception e) {
      // todo: exception
      throw new RuntimeException(e);
    }
  }

  public JavaDStream createSource(JavaStreamingContext streamingContext) {

    return kafkaSource.createSource(streamingContext);
  }
}