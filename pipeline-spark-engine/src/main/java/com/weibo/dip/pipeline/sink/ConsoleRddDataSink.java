package com.weibo.dip.pipeline.sink;

import java.util.Iterator;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

/**
 * 写出到标准输入输出中
 */
public class ConsoleRddDataSink extends JavaRddDataSink {

  public ConsoleRddDataSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(JavaRDD<Row> rdd) {
    rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
      public void call(Iterator<Row> rowIterator) throws Exception {
        while (rowIterator.hasNext()) {
          Row row = rowIterator.next();
          System.out.println(parser.parseRow(row));
        }
      }
    });
  }

  @Override
  public void stop() {

  }
}
