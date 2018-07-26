package com.weibo.dip.pipeline.sink;

import java.util.Iterator;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

/**
 * Create by hongxun on 2018/7/26
 */
public abstract class JavaRddDataSink extends RddDataSink {

  public JavaRddDataSink(Map<String, Object> params) {
    super(params);
  }

  public abstract void write(JavaRDD<Row> rdd);
}


class KafkaRddDataSink extends JavaRddDataSink {

  public KafkaRddDataSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(JavaRDD<Row> rdd) {

  }
}

class ConsoleRddDataSink extends JavaRddDataSink {

  public ConsoleRddDataSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(JavaRDD<Row> rdd) {
    rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
      public void call(Iterator<Row> rowIterator) throws Exception {
        while (rowIterator.hasNext()) {
          Row row = rowIterator.next();
          StringBuffer sb = new StringBuffer();
          for (int i = 0; i < row.length(); i++) {
            Object obj = row.get(i);
            if (obj != null) {
              sb.append(obj);
            }
            sb.append(",");
          }
          System.out.println(sb);
        }
      }
    });
  }
}
