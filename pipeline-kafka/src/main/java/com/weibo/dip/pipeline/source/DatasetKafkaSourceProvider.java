package com.weibo.dip.pipeline.source;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * kafka 不同版本Dataset的source生成器，由各版本分别继承生成source
 * Create by hongxun on 2018/7/30
 */
public abstract class DatasetKafkaSourceProvider implements Serializable {

  /**
   * 记录版本号，每个实现类都要赋值
   */
  protected String version;

  /**
   * 加载所有service的类
   */
  private static ServiceLoader<DatasetKafkaSourceProvider> providerServiceLoader = ServiceLoader
      .load(DatasetKafkaSourceProvider.class);


  /**
   * 获取最大版本的provider实例
   */
  public static DatasetKafkaSourceProvider newInstance() {
    // todo: 获取KafkaSourceProvider，取版本最大
    Iterator<DatasetKafkaSourceProvider> iterator = providerServiceLoader.iterator();
    if (iterator.hasNext()) {
      return iterator.next();
    } else {
      return null;
    }
  }

  /**
   * source生成方法，供子类实现
   *
   * @param params source配置
   * @return 对应的DatasetKafkaSource
   * @throws Exception 异常
   */
  public abstract DatasetSource createDataSource(Map<String, Object> params);
}
