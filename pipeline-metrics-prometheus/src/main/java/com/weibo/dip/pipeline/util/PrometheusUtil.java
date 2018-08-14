package com.weibo.dip.pipeline.util;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;
import java.io.IOException;
import java.io.StringWriter;

/**
 * Create by hongxun on 2018/8/12
 */
public class PrometheusUtil {

  public static CollectorRegistry dropwizardToPrometheus(MetricRegistry metricRegistry) {
    CollectorRegistry collectorRegistry = new CollectorRegistry();
    new DropwizardExports(metricRegistry).register(collectorRegistry);
    return collectorRegistry;
  }

  public static String prometheustoString(CollectorRegistry collectorRegistry) {
    StringWriter stringWriter = new StringWriter();
    try {
      TextFormat.write004(stringWriter, collectorRegistry.metricFamilySamples());
      return stringWriter.getBuffer().toString();
    } catch (IOException e) {
    } finally {
      try {
        stringWriter.close();
      } catch (IOException e) {
      }
    }
    return "";

  }

}
