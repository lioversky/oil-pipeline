package com.weibo.dip.pipeline.sink;

import java.util.Map;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka 0.8版本同步sink
 * Create by hongxun on 2018/8/1
 */
public class Kafka08DataSyncSink extends KafkaDataSink {

  private transient Producer<String, String> producer;
  private String topic;
  private Map<String, Object> kafkaParams;

  public Kafka08DataSyncSink(Map<String, Object> params) {
    super(params);
    topic = (String) params.get("topic");
    kafkaParams = (Map<String, Object>) params.get("options");
    producer = new KafkaProducer(kafkaParams);
  }

  @Override
  public void write(String msg) {
    write(topic, msg);
  }

  @Override
  public void write(String _topic, String msg) {
    producer.send(new ProducerRecord<>(_topic, msg),
        new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata != null) {
              System.out.println(
                  "offset: " + metadata.offset() + ", partition: " + metadata.partition()
                      + ", message: " + msg);
            } else {
              exception.printStackTrace();
            }
          }
        });
  }

  @Override
  public void stop() {
    if (producer != null) {
      producer.close();
    }
  }
}
