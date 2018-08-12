package com.weibo.dip.pipeline.clients;

import com.weibo.dip.pipeline.provider.KafkaProvider;
import com.weibo.dip.pipeline.sink.KafkaDataSink;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * kafka 不同版本的sink生成器，由各版本分别继承生成source
 * Create by hongxun on 2018/8/1
 */
public abstract class KafkaProducerProvider extends KafkaProvider {

  /**
   * 创建sink的抽象方法
   *
   * @param params 配置参数
   */
  public abstract PipelineKafkaProducer createProducer(Map<String, Object> params);

  /**
   * 加载所有service的类
   */
  private static ServiceLoader<KafkaProducerProvider> producerServiceLoader = ServiceLoader
      .load(KafkaProducerProvider.class);

  /**
   * 获取最大版本的provider实例
   */
  public static synchronized KafkaProducerProvider newInstance() {
    double maxVersion = 0;
    KafkaProducerProvider maxProvider = null;

    Iterator<KafkaProducerProvider> iterator = producerServiceLoader.iterator();
    while (iterator.hasNext()) {
      KafkaProducerProvider provider = iterator.next();
      if (provider.getVersion() > maxVersion) {
        maxProvider = provider;
        maxVersion = provider.getVersion();
      }
    }
    return maxProvider;
  }

}
