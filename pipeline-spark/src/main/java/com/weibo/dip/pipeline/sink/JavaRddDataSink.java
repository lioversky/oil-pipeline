package com.weibo.dip.pipeline.sink;

import java.util.Iterator;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

/**
 * 对rdd结构数据的输出抽象类
 * Create by hongxun on 2018/7/26
 */
public abstract class JavaRddDataSink extends RddDataSink {

  public JavaRddDataSink(Map<String, Object> params) {
    super(params);
  }

  public abstract void write(JavaRDD<Row> rdd);
}

/**
 * 写出到kafka
 */
class KafkaRddDataSink extends JavaRddDataSink {

  public KafkaRddDataSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(JavaRDD<Row> rdd) {

  }
}

/**
 * 写出到标准输入输出中
 */
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
