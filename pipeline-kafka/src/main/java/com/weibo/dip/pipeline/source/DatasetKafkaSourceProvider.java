package com.weibo.dip.pipeline.source;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * Create by hongxun on 2018/7/30
 */
public abstract class DatasetKafkaSourceProvider implements Serializable {

  protected String version;

  private static ServiceLoader<DatasetKafkaSourceProvider> providerServiceLoader = ServiceLoader
      .load(DatasetKafkaSourceProvider.class);


  public static DatasetKafkaSourceProvider newInstance() {
    // todo: 获取KafkaSourceProvider，取版本最大
    Iterator<DatasetKafkaSourceProvider> iterator = providerServiceLoader.iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      return null;
    }
  }

  public abstract DatasetSource createDataSource(Map<String, Object> params) throws Exception;
}
