package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.metrics.MetricsSystem;
import com.weibo.dip.pipeline.util.KafkaProducerUtil;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * kafka 同步sink
 * Create by hongxun on 2018/8/1
 */
public class KafkaDataSyncSink extends KafkaDataSink {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataSyncSink.class);

  public KafkaDataSyncSink(Map<String, Object> params) {
    super(params);
    LOGGER.info(String.format("Create KafkaDataSyncSink for topic: %s, kafkaParams: %s", topic,
        kafkaParams.toString()));
  }

  @Override
  public void write(String msg) {
    write(topic, msg);
  }

  @Override
  public void write(String kafkaTopic, String msg) {
    KafkaProducerUtil.getProducer(kafkaParams).send(kafkaTopic, msg, (success, exception) -> {
      if (success) {
        MetricsSystem.getCounter("kafka_send_sync_" + kafkaTopic).inc();
      } else {
        MetricsSystem.getCounter("kafka_send_error_sync_" + kafkaTopic).inc();
      }
    });
  }

  @Override
  public void stop() {
    KafkaProducerUtil.closeProducer(kafkaParams);
    LOGGER.info(String.format("Stop KafkaDataSyncSink for topic: %s", topic));
  }
}
