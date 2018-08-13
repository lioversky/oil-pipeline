package com.weibo.dip.pipeline.util;


import com.weibo.dip.pipeline.provider.KafkaProducerProvider;
import com.weibo.dip.pipeline.clients.PipelineKafkaProducer;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 同步发送kafka工具类
 * Created by hongxun on 16/6/7.
 */
public class KafkaProducerUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerUtil.class);
  private static Map<Map<String, Object>, PipelineKafkaProducer<String, String>> producerPool = new HashMap<>();

  private static Object lock = new Object();
  private static KafkaProducerProvider producerProvider = KafkaProducerProvider.newInstance();

  private static PipelineKafkaProducer<String, String> createProducer(Map<String, Object> config) {
    PipelineKafkaProducer<String, String> producer = null;

    synchronized (lock) {
      if (producerPool.get(config) == null) {
        producer = producerPool.get(config);
        if (producer == null) {
          producer = producerProvider.createProducer(config);
          producerPool.put(config, producer);

        }
        return producer;

      } else {
        return producerPool.get(config);
      }
    }
  }


  public static PipelineKafkaProducer<String, String> getProducer(Map<String, Object> config) {
    if (!producerPool.containsKey(config)) {
      return createProducer(config);
    }
    return producerPool.get(config);
  }

  public static void closeProducer(Map<String, Object> config) {
    if (producerPool.containsKey(config)) {
      producerPool.remove(config).stop();
    }
  }
}
