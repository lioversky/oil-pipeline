package com.weibo.dip.pipeline.util;


import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 同步发送kafka工具类
 * Created by hongxun on 16/6/7.
 */
public class KafkaProducerUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerUtil.class);
  private static Map<Map<String, Object>, Producer<String, String>> producerPool = new HashMap<>();

  private static Object lock = new Object();


  private static Producer<String, String> createProducer(Map<String, Object> config) {
    Producer<String, String> producer = null;

    synchronized (lock) {
      if (producerPool.get(config) == null) {
        producer = producerPool.get(config);
        if (producer == null) {
          producer = new KafkaProducer(config);
          producerPool.put(config, producer);

        }
        return producer;

      } else {
        return producerPool.get(config);
      }
    }
  }


  public static Producer<String, String> getProducer(Map<String, Object> config) {
    if (!producerPool.containsKey(config)) {
      return createProducer(config);
    }
    return producerPool.get(config);
  }

}
