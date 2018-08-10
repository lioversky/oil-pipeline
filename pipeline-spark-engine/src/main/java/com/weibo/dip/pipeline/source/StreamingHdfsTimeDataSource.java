package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


/**
 * 按batch time读取对应时间段的文件，生成DStream
 * Create by hongxun on 2018/7/5
 */
public class StreamingHdfsTimeDataSource extends StreamingDataSource {

  // todo: HdfsTime Dstream
  public StreamingHdfsTimeDataSource(Map map) {
    super(map);
  }

  @Override
  public void stop() {

  }

  @Override
  public JavaDStream createSource(JavaStreamingContext streamingContext) {
    return null;
  }
}

