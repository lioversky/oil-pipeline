package com.weibo.dip.pipeline.source;

import java.util.Map;

/**
 * 0.10版本kafka的Dataset source生成器
 * Create by hongxun on 2018/7/30
 */
public class DatasetKafka010SourceProvider extends DatasetKafkaSourceProvider {

  protected String version = "0.10";
  @Override
  public DatasetSource createDataSource(Map<String, Object> params) {
    return new DatasetKafka010DataSource(params);
  }

  @Override
  public double getVersion() {
    return 0.10;
  }
}
