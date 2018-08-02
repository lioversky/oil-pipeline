package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.util.KafkaProducerUtil;
import java.util.Map;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * kafka 0.10版本同步sink
 * Create by hongxun on 2018/8/1
 */
public class Kafka010DataSyncSink extends KafkaDataSink {

  private String topic;
  private Map<String, Object> kafkaParams;

  public Kafka010DataSyncSink(Map<String, Object> params) {
    super(params);
    topic = (String) params.get("topic");
    kafkaParams = (Map<String, Object>) params.get("options");
  }

  @Override
  public void write(String msg) {
    write(topic, msg);
  }

  @Override
  public void write(String _topic, String msg) {
    KafkaProducerUtil.getProducer(kafkaParams).send(new ProducerRecord<>(_topic, msg),
        new Callback() {
          //todo:回调函数中记录成功率
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
    //todo: 什么时候stop掉producer
  }
}
