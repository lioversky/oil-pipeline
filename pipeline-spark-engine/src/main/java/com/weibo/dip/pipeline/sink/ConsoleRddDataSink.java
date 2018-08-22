package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.metrics.MetricsSystem;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

/**
 * 写出到标准输入输出中
 */
public class ConsoleRddDataSink extends JavaRddDataSink {

  /**
   * 构造函数
   *
   * @param params 参数
   */
  public ConsoleRddDataSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(JavaRDD<Row> rdd) {
    Long count = rdd.count();
    List<Row> rowList = rdd.take(new Long(count > 20 ? 20 : count).intValue());
    for (Row row : rowList) {
      try {
        System.out.println(parser.parseRow(row));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    MetricsSystem.getCounter(metricsName).inc(count);
    /*rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
      public void call(Iterator<Row> rowIterator) throws Exception {
        while (rowIterator.hasNext()) {
          Row row = rowIterator.next();
          System.out.println(parser.parseRow(row));
          MetricsSystem.getCounter(metricsName).inc();
        }

      }
    });*/
  }

  @Override
  public void stop() {

  }
}
