package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Create by hongxun on 2018/7/5
 */
public class StreamingHdfsTimeDataSource extends StreamingDataSource {

  public StreamingHdfsTimeDataSource(Map map) {
    super(map);
  }

  @Override
  public JavaDStream createSource(JavaStreamingContext streamingContext) {
    return null;
  }
}
