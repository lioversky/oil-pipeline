package com.weibo.dip.pipeline.source;

import java.util.Map;

/**
 * Create by hongxun on 2018/7/27
 */
public class StreamingKafka010SourceProvider extends StreamingKafkaSourceProvider {

  protected String version = "0.10.0";
  @Override
  public StreamingDataSource createDataSource(Map<String, Object> params) throws Exception {
    return new StreamingKafka010DataSource(params);
  }
}
