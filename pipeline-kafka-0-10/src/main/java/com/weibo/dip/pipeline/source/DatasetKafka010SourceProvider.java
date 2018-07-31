package com.weibo.dip.pipeline.source;

import java.util.Map;

/**
 * Create by hongxun on 2018/7/30
 */
public class DatasetKafka010SourceProvider extends DatasetKafkaSourceProvider {

  protected String version = "0.10.0";
  @Override
  public DatasetSource createDataSource(Map<String, Object> params) throws Exception {
    return new DatasetKafka010DataSource(params);
  }
}
