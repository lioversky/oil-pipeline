package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.provider.KafkaProvider;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * kafka 不同版本的sink生成器，由各版本分别继承生成source
 * Create by hongxun on 2018/8/1
 */
public abstract class KafkaSinkProvider extends KafkaProvider {

  /**
   * 创建sink的抽象方法
   *
   * @param params 配置参数
   */
  public abstract KafkaDataSink createDataSink(Map<String, Object> params);

  /**
   * 加载所有service的类
   */
  private static ServiceLoader<KafkaSinkProvider> providerServiceLoader = ServiceLoader
      .load(KafkaSinkProvider.class);

  /**
   * 获取最大版本的provider实例
   */
  public static synchronized KafkaSinkProvider newInstance() {
    double maxVersion = 0;
    KafkaSinkProvider maxProvider = null;

    Iterator<KafkaSinkProvider> iterator = providerServiceLoader.iterator();
    while (iterator.hasNext()) {
      KafkaSinkProvider provider = iterator.next();
      if (provider.getVersion() > maxVersion) {
        maxProvider = provider;
        maxVersion = provider.getVersion();
      }
    }
    return maxProvider;
  }

}
