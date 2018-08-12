package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.metrics.MetricsSystem;
import com.weibo.dip.pipeline.util.KafkaProducerUtil;
import java.util.Map;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka 0.8版本同步sink
 * Create by hongxun on 2018/8/1
 */
public class Kafka08DataSyncSink extends KafkaDataSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(Kafka08DataSyncSink.class);

  public Kafka08DataSyncSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(String msg) {
    write(topic, msg);
  }

  @Override
  public void write(String _topic, String msg) {
    KafkaProducerUtil.getProducer(kafkaParams).send(new ProducerRecord<>(_topic, msg),
        new Callback() {
          @Override
          public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (metadata != null) {
              String topicMetricsName = metricsName + "_" + _topic;
              MetricsSystem.getCounter(topicMetricsName).inc();
              LOGGER.info(
                  "offset: " + metadata.offset() + ", partition: " + metadata.partition()
                      + ", message: " + msg);
            } else {
              String errorMetricsName = metricsName + "_error_" + _topic;
              MetricsSystem.getCounter(errorMetricsName).inc();
              LOGGER.error(String.format("Send kafka to topic: %s error!", _topic), exception);

            }
          }
        });
  }

  @Override
  public void stop() {
    //todo: 什么时候stop掉producer
  }
}
