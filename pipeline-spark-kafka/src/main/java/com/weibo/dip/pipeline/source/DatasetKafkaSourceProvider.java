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
  protected String version = "0.7";

  /**
   * 加载所有service的类
   */
  private static ServiceLoader<DatasetKafkaSourceProvider> providerServiceLoader = ServiceLoader
      .load(DatasetKafkaSourceProvider.class);


  /**
   * 获取最大版本的provider实例
   */
  public static DatasetKafkaSourceProvider newInstance() {
    double maxVersion = 0;
    DatasetKafkaSourceProvider maxProvider = null;

    Iterator<DatasetKafkaSourceProvider> iterator = providerServiceLoader.iterator();
    while (iterator.hasNext()) {
      DatasetKafkaSourceProvider provider = iterator.next();
      if (Double.parseDouble(provider.version) > maxVersion) {
        maxProvider = provider;
        maxVersion = Double.parseDouble(provider.version);
      }
    }
    return maxProvider;
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
