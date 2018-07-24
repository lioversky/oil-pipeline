package com.weibo.dip.pipeline.spark;

import com.weibo.dip.pipeline.runner.Runner;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * 支持多数据源.
 * Create by hongxun on 2018/7/5
 */

public class SparkRunner extends Runner {

  private SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();

  @Override
  public void start() throws Exception {

  }

  private void process() {
    Dataset<Row> dataset = sparkSession.sql("");
    dataset.withColumn("", functions.col(""));
  }


  private Dataset<Row> loadSparkDataSet(String format, Map<String, String> options) {
    return sparkSession
        .read()
        .format(format)
        .options(options).load();
  }

  private void write(Dataset dataset, String mode, String format, Map<String, String> options) {
    dataset.write()
        .mode(mode)
        .format(format)
        .options(options)
        .save();
  }

  @Override
  public void stop() throws Exception {

  }
}
