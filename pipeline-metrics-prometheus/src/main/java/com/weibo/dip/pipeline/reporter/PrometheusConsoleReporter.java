package com.weibo.dip.pipeline.reporter;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.weibo.dip.pipeline.reporter.PushGatewayReporter.Builder;
import com.weibo.dip.pipeline.util.PrometheusUtil;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Create by hongxun on 2018/8/13
 */
public class PrometheusConsoleReporter extends ScheduledReporter {

  private final Clock clock;
  private PrintStream output;
  private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public PrometheusConsoleReporter(MetricRegistry registry, String name,
      MetricFilter filter, TimeUnit rateUnit,
      TimeUnit durationUnit,Clock clock, PrintStream output) {
    super(registry, name, filter, rateUnit, durationUnit);
    this.output = output;
    this.clock = clock;
  }

  @Override
  public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {
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
    final String dateTime = dateFormat.format(new Date(clock.getTime()));
    printWithBanner(dateTime, '=');
    output.println(
        PrometheusUtil.prometheustoString(PrometheusUtil.dropwizardToPrometheus(registry)));
    output.flush();
  }

  private void printWithBanner(String s, char c) {
    output.print(s);
    output.print(' ');
    for (int i = 0; i < (80 - s.length() - 1); i++) {
      output.print(c);
    }
    output.println();
  }

  public static class Builder {

    private final MetricRegistry registry;
    private Clock clock;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private MetricFilter filter;
    private PrintStream output;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
      this.clock = Clock.defaultClock();
      this.rateUnit = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter = MetricFilter.ALL;
      this.output = System.out;
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

    public Builder outputTo(PrintStream output) {
      this.output = output;
      return this;
    }


    public PrometheusConsoleReporter build() {
      return new PrometheusConsoleReporter(registry,
          "Prometheus-Reporter",
          filter,
          rateUnit,
          durationUnit,
          clock, output);
    }
  }
}
