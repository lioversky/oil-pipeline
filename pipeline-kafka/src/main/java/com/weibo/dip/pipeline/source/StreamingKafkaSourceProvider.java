package com.weibo.dip.pipeline.source;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Create by hongxun on 2018/7/27
 */
public abstract class StreamingKafkaSourceProvider implements Serializable {

  protected String version;

  private static ServiceLoader<StreamingKafkaSourceProvider> providerServiceLoader = ServiceLoader
      .load(StreamingKafkaSourceProvider.class);

  public static StreamingKafkaSourceProvider newInstance() {
    // todo: 获取KafkaSourceProvider，取版本最大
    Iterator<StreamingKafkaSourceProvider> iterator = providerServiceLoader.iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      return null;
    }
  }

  public abstract StreamingDataSource createDataSource(Map<String, Object> params) throws Exception;
}
