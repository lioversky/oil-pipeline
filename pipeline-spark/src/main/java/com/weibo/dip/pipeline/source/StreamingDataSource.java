package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Create by hongxun on 2018/7/5
 */
public abstract class StreamingDataSource<T> {

  public StreamingDataSource(Map<String, Object> map) {
  }

  public abstract JavaDStream<T> createSource(JavaStreamingContext streamingContext);

}
