package com.weibo.dip.pipeline.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.weibo.dip.pipeline.clients.KafkaProducerProvider;
import com.weibo.dip.pipeline.clients.PipelineKafkaProducer;
import com.weibo.dip.pipeline.metrics.MetricsSystem;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步发送kafka工具类
 * Create by hongxun on 2018/8/2
 */
public class AsyncKafkaProducerUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncKafkaProducerUtil.class);
  private static BlockingQueue<TopicAndMsg> queue = new LinkedBlockingQueue<>(500000);
  private static Map<String, PipelineKafkaProducer<String, String>> topicProducerMap = new HashMap<>();
  private static Map<Map<String, Object>, PipelineKafkaProducer<String, String>> producerPool = new HashMap<>();
  private static Object lock = new Object();
  private static KafkaProducerProvider producerProvider = KafkaProducerProvider.newInstance();

  static {
    ExecutorService pool = Executors.newFixedThreadPool(
        5,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Kafka-producer" + "-%d").build());
    for (int i = 0; i < 5; i++) {
      pool.execute(new SendThread());
    }

  }

  public static void createProducer(String _topic, Map<String, Object> config) {
    synchronized (lock) {
      if (topicProducerMap.get(_topic) == null) {

        PipelineKafkaProducer<String, String> producer = producerPool.get(config);
        if (producer == null) {
          producer = producerProvider.createProducer(config);
          producerPool.put(config, producer);
        }
        topicProducerMap.put(_topic, producer);
      }
    }
  }

  public static PipelineKafkaProducer<String, String> getProducer(String topic) {
    return topicProducerMap.get(topic);
  }


  public static void addMessage(String topic, String msg) throws InterruptedException {
    queue.put(new TopicAndMsg(topic, msg));
  }

  /**
   * 消息放入本地队列，如果不存在topic对应的producer，创建
   *
   * @param topic 发送到topic
   * @param msg 消息
   * @param kafkaParams producer配置
   */
  public static void addMessage(String topic, String msg, Map<String, Object> kafkaParams)
      throws InterruptedException {
    if (!topicProducerMap.containsKey(topic)) {
      createProducer(topic, kafkaParams);
    }
    queue.put(new TopicAndMsg(topic, msg));
  }

  static class TopicAndMsg implements Serializable {

    String topic;
    String msg;

    public TopicAndMsg(String topic, String msg) {
      this.msg = msg;
      this.topic = topic;
    }
  }

  /**
   * 处理线程
   */
  static class SendThread extends Thread {

    public void run() {
      LOGGER.warn(Thread.currentThread().getName() + " Starting!!!");

      while (true) {
        try {
          TopicAndMsg topicAndMsg = queue.take();
          if (topicAndMsg != null) {
            PipelineKafkaProducer p = getProducer(topicAndMsg.topic);
            if (p != null) {
              p.send(topicAndMsg.topic, topicAndMsg.msg, (success, exception) -> {
                if (success) {
                  MetricsSystem.getCounter("kafka_send_async_" + topicAndMsg.topic).inc();
                } else {
                  MetricsSystem.getCounter("kafka_send_async_" + topicAndMsg.topic).inc();
                }
              });
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

}
