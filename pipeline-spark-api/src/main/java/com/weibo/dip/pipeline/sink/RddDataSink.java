package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.Sink;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;

/**
 * Create by hongxun on 2018/7/26
 */
public abstract class RddDataSink extends Sink<JavaRDD<Row>> {

  public RddDataSink(Map<String, Object> params) {
  }

  public abstract void write(JavaRDD<Row> rdd);
}
