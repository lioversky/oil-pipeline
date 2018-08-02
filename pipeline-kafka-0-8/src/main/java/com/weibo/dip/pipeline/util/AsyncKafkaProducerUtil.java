package com.weibo.dip.pipeline.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步发送kafka工具类
 * Create by hongxun on 2018/8/2
 */
public class AsyncKafkaProducerUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(AsyncKafkaProducerUtil.class);
  private static BlockingQueue<TopicAndMsg> queue = new LinkedBlockingQueue<>(500000);
  private static Map<String, Producer<String, String>> topicProducerMap = new HashMap<String, Producer<String, String>>();
  private static Map<Map<String, Object>, Producer<String, String>> producerPool = new HashMap<>();
  private static Object lock = new Object();

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

        Producer<String, String> producer = producerPool.get(config);
        if (producer == null) {
          producer = new KafkaProducer(config);
          producerPool.put(config, producer);
        }
        topicProducerMap.put(_topic, producer);
      }
    }
  }

  public static Producer<String, String> getProducer(String topic) {
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
            Producer p = getProducer(topicAndMsg.topic);

            if (p != null) {
              p.send(new ProducerRecord<String, String>(topicAndMsg.topic, topicAndMsg.msg),
                  new Callback() {
                    //todo:回调函数中记录成功率
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                      if (metadata != null) {
                        System.out.println(
                            "async offset: " + metadata.offset() + ", partition: " + metadata.partition()
                                + ", message: " + topicAndMsg.msg);
                      } else {
                        exception.printStackTrace();
                      }
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
