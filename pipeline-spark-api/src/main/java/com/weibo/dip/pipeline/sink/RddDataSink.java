package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.Sink;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;

/**
 * RDD输出抽象类，DStream通过foreachRDD也调用此输出
 * Create by hongxun on 2018/7/26
 */
public abstract class RddDataSink extends Sink<JavaRDD<Row>> {

  public RddDataSink(Map<String, Object> params) {
  }

  /**
   * 写出抽象方法，供子类实现
   *
   * @param rdd 数据
   */
  public abstract void write(JavaRDD<Row> rdd);
}
