package com.weibo.dip.pipeline.clients;

import java.util.Map;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka 0.8版本producer
 * Create by hongxun on 2018/8/12
 */
public class Kafka08Producer extends PipelineKafkaProducer<String, String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(Kafka08Producer.class);
  private Producer<String, String> producer;

  public Kafka08Producer(Map<String, Object> config) {
    super(config);
    producer = new KafkaProducer(config);
  }

  @Override
  public void send(String topic, Object data, KafkaCallback callback) {
    String msg = (String) data;
    producer.send(new ProducerRecord<>(topic, msg), (metadata, exception) -> {
      if (metadata != null) {
        if (callback != null) {
          callback.onCompletion(true, null);
        }
        LOGGER.info(
            "offset: " + metadata.offset() + ", partition: " + metadata.partition()
                + ", message: " + msg);
      } else {
        callback.onCompletion(false, exception);
        LOGGER.error(String.format("Send kafka to topic: %s error!", topic), exception);

      }
    });
  }

  @Override
  public void send(String topic, Object msg) {
    send(topic, msg, null);
  }

  @Override
  public void stop() {

  }
}
