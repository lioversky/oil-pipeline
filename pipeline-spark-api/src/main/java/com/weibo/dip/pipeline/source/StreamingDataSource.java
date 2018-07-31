package com.weibo.dip.pipeline.source;

import com.weibo.dip.pipeline.Source;
import java.io.Serializable;
import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * 读取DStream的Source的抽象类.
 * Create by hongxun on 2018/7/5
 */
public abstract class StreamingDataSource<T> extends Source {

  public StreamingDataSource(Map<String, Object> params) {
  }

  public abstract JavaDStream<T> createSource(JavaStreamingContext streamingContext);

}
