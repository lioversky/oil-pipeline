package com.weibo.dip.pipeline.source;

import com.weibo.dip.pipeline.provider.KafkaProvider;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * kafka 不同版本Streaming的source生成器，由各版本分别继承生成source
 * Create by hongxun on 2018/7/27
 */
public abstract class StreamingKafkaSourceProvider extends KafkaProvider {

  /**
   * 加载所有service的类
   */
  private static ServiceLoader<StreamingKafkaSourceProvider> providerServiceLoader = ServiceLoader
      .load(StreamingKafkaSourceProvider.class);

  /**
   * 获取最大版本的provider实例
   */
  public static StreamingKafkaSourceProvider newInstance() {
    double maxVersion = 0;
    StreamingKafkaSourceProvider maxProvider = null;

    Iterator<StreamingKafkaSourceProvider> iterator = providerServiceLoader.iterator();
    while (iterator.hasNext()) {
      StreamingKafkaSourceProvider provider = iterator.next();
      if (provider.getVersion() > maxVersion) {
        maxProvider = provider;
        maxVersion = provider.getVersion();
      }
    }
    return maxProvider;
  }

  /**
   * source生成方法，供子类实现
   *
   * @param params source配置
   * @return 对应的streaming KafkaSource
   * @throws Exception 异常
   */
  public abstract StreamingDataSource createDataSource(Map<String, Object> params);
}
