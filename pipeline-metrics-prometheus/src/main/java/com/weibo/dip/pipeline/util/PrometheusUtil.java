package com.weibo.dip.pipeline.util;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.Collector.Type;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.common.TextFormat;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
      write004(stringWriter, collectorRegistry.metricFamilySamples());
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

  public static void write004(Writer writer, Enumeration<Collector.MetricFamilySamples> mfs)
      throws IOException {
    /* See http://prometheus.io/docs/instrumenting/exposition_formats/
     * for the output format specification. */
    while (mfs.hasMoreElements()) {
      Collector.MetricFamilySamples metricFamilySamples = mfs.nextElement();
      String metricName = splitAppName(metricFamilySamples.name);
      writer.write("# HELP ");
      writer.write(metricName);
      writer.write(' ');
      writeEscapedHelp(writer, metricFamilySamples.help);
      writer.write('\n');

      writer.write("# TYPE ");
      writer.write(metricName);
      writer.write(' ');
      writer.write(typeString(metricFamilySamples.type));
      writer.write('\n');

      for (Collector.MetricFamilySamples.Sample sample : metricFamilySamples.samples) {
        writer.write(splitAppName(sample.name));
        if (sample.labelNames.size() > 0) {
          writer.write('{');
          for (int i = 0; i < sample.labelNames.size(); ++i) {
            writer.write(sample.labelNames.get(i));
            writer.write("=\"");
            writeEscapedLabelValue(writer, sample.labelValues.get(i));
            writer.write("\",");
          }
          writer.write('}');
        }
        writer.write(' ');
        writer.write(Collector.doubleToGoString(sample.value));
        if (sample.timestampMs != null) {
          writer.write(' ');
          writer.write(sample.timestampMs.toString());
        }
        writer.write('\n');
      }
    }
  }

  private static void writeEscapedHelp(Writer writer, String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\':
          writer.append("\\\\");
          break;
        case '\n':
          writer.append("\\n");
          break;
        default:
          writer.append(c);
      }
    }
  }

  private static void writeEscapedLabelValue(Writer writer, String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '\\':
          writer.append("\\\\");
          break;
        case '\"':
          writer.append("\\\"");
          break;
        case '\n':
          writer.append("\\n");
          break;
        default:
          writer.append(c);
      }
    }
  }

  private static String typeString(Collector.Type t) {
    switch (t) {
      case GAUGE:
        return "gauge";
      case COUNTER:
        return "counter";
      case SUMMARY:
        return "summary";
      case HISTOGRAM:
        return "histogram";
      default:
        return "untyped";
    }
  }

  private static Pattern p = Pattern.compile("^application_\\d+_\\d+_(.*)");

  private static String splitAppName(String name) {
    Matcher matcher = p.matcher(name);
    if (matcher.find()) {
      return matcher.group(1);
    } else {
      return name;
    }

  }
}
