package com.weibo.dip.pipeline.reporter;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.weibo.dip.pipeline.util.PrometheusUtil;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Create by hongxun on 2018/8/12
 */
public class PushGatewayReporter extends ScheduledReporter {

  private PushGateway pushGateway;
  private String jobName;
  private static final Logger LOGGER = LoggerFactory.getLogger(PushGatewayReporter.class);

  private final Clock clock;

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public PushGatewayReporter(MetricRegistry registry, String name,
      MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit, Properties kafkaConfig, Clock clock, String gatewayUrl,
      String jobName) {
    super(registry, name, filter, rateUnit, durationUnit);
    this.clock = clock;
    this.jobName = jobName;
    pushGateway = new PushGateway(gatewayUrl);
  }


  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
    try {
      MetricRegistry registry = new MetricRegistry();
      if (!gauges.isEmpty()) {
        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
          registry.register(entry.getKey(), entry.getValue());
        }
      }
      if (!counters.isEmpty()) {
        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
          registry.register(entry.getKey(), entry.getValue());
        }
      }
      if (!histograms.isEmpty()) {
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
          registry.register(entry.getKey(), entry.getValue());
        }
      }
      if (!meters.isEmpty()) {
        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
          registry.register(entry.getKey(), entry.getValue());
        }
      }
      if (!timers.isEmpty()) {
        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
          registry.register(entry.getKey(), entry.getValue());
        }
      }

      pushGateway.pushAdd(PrometheusUtil.dropwizardToPrometheus(registry), jobName);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static class Builder {

    private final MetricRegistry registry;
    private Clock clock;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private String gatewayUrl;
    private String jobName;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.clock = Clock.defaultClock();
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
    }


    public Builder withClock(Clock clock) {
      this.clock = clock;
      return this;
    }

    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }


    public Builder withJobName(String jobName) {
      this.jobName = jobName;
      return this;
    }

    public Builder withGatewayUrl(String gatewayUrl) {
      this.gatewayUrl = gatewayUrl;
      return this;
    }


    public PushGatewayReporter build(Properties config) {
      return new PushGatewayReporter(registry,
          "Prometheus-Reporter",
          filter,
          rateUnit,
          durationUnit,
          config,
          clock, gatewayUrl, jobName);
    }
  }

}
