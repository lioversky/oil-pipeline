package com.weibo.dip.pipeline.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.sink.PipelineKafkaProducer;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by hongxun on 2018/7/4
 */
public class KafkaReporter extends ScheduledReporter {


  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReporter.class);

  private PipelineKafkaProducer producer;
  private final Clock clock;
  private ObjectMapper objectMapper;
  private String topic;
  private String business;
  private Properties kafkaConfig;

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }


  public KafkaReporter(MetricRegistry registry, String name,
      MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit, Properties kafkaConfig, Clock clock) {
    super(registry, name, filter, rateUnit, durationUnit);
    this.clock = clock;
    this.objectMapper = new ObjectMapper().registerModule(
        new MetricsModule(rateUnit, durationUnit, false));
    this.topic = (String) kafkaConfig.remove("topic");
    this.business = (String) kafkaConfig.remove("business");

    this.kafkaConfig = new Properties();
    this.kafkaConfig.put("bootstrap.servers", kafkaConfig.getProperty("servers"));
    this.kafkaConfig
        .put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    this.kafkaConfig
        .put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    this.kafkaConfig.put("acks", "0");
    //    在无法连接kafka时不会阻塞
    this.kafkaConfig.put("metadata.fetch.timeout.ms", "1000");
    //todo:创建实例
    //    producer = new KafkaProducer(this.kafkaConfig);
  }

  /**
   * 将指标和business写到Map中转成json，写到topic
   */
  @Override
  public void report(SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {

    final long timestamp = clock.getTime();
    String json = null;

    try {
      String appName = null;
      Map<String, Object> map = Maps.newHashMap();
      if (!gauges.isEmpty()) {
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
          String key = entry.getKey();
          String newKey = key.substring(key.indexOf(".") + 1);

          String[] splits = newKey.split("\\.");
          if (appName == null && splits.length > 2 && "StreamingMetrics".equals(splits[2])) {
            appName = splits[1];
          }
          map.put(newKey, entry.getValue());
        }
      }
      if (!counters.isEmpty()) {
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
          String key = entry.getKey();
          String newKey = key.substring(key.indexOf(".") + 1);
          map.put(newKey, entry.getValue());
        }
      }
      if (!histograms.isEmpty()) {
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
          String key = entry.getKey();
          String newKey = key.substring(key.indexOf(".") + 1);
          map.put(newKey, entry.getValue());
        }
      }
      if (!meters.isEmpty()) {
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
          String key = entry.getKey();
          String newKey = key.substring(key.indexOf(".") + 1);
          map.put(newKey, entry.getValue());
        }
      }
      if (!timers.isEmpty()) {
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
          String key = entry.getKey();
          String newKey = key.substring(key.indexOf(".") + 1);
          map.put(newKey, entry.getValue());
        }
      }

      map.put("timestamp", timestamp);
      map.put("business", business);
      map.put("appname", appName);
      json = objectMapper.writeValueAsString(map);
    } catch (Exception e) {
      LOGGER.warn("Parse metrics to json error", e);
    }
    if (json != null) {
      try {
//        if (producer == null) {
//          producer = new KafkaProducer(this.kafkaConfig);
//        }
        producer.send(topic, json);

      } catch (Exception e) {
        LOGGER.warn("Unable to report to Kafka", e);
        try {
          producer.stop();
        } catch (Exception e1) {
          LOGGER.warn("Error closing Kafka", e1);
        } finally {
          producer = null;
        }
      }
    }
  }

  @Override
  public void stop() {
    try {
      super.stop();
    } finally {
      producer.stop();
    }

  }

  public static class Builder {

    private final MetricRegistry registry;
    private Clock clock;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.clock = Clock.defaultClock();
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }


    public KafkaReporter.Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public KafkaReporter.Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public KafkaReporter.Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    public KafkaReporter.Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public KafkaReporter build(Properties config) {
      return new KafkaReporter(registry,
          "Kafka-Reporter",
          filter,
          rateUnit,
          durationUnit,
          config,
          clock);
    }
  }

}
