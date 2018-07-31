package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * Create by hongxun on 2018/7/30
 */

class DatasetKafkaDataSource extends DatasetSource {

  private DatasetSource datasetSource;
  private DatasetKafkaSourceProvider kafkaSourceProvider = DatasetKafkaSourceProvider.newInstance();

  public DatasetKafkaDataSource(Map<String, Object> params) {
    super(params);
    try {
      datasetSource = kafkaSourceProvider.createDataSource(params);
    } catch (Exception e) {
      // todo: exception
      throw new RuntimeException(e);
    }
  }

  @Override
  public Dataset createSource(SparkSession sparkSession) {
    return datasetSource.createSource(sparkSession);
  }
}

class DatasetFileSource extends DatasetSource {


  protected String sourceFormat;
  protected Map<String, String> sourceOptions;

  public DatasetFileSource(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset createSource(SparkSession sparkSession) {
    return sparkSession
        .read()
        .format(sourceFormat)
        .options(sourceOptions).load();

  }
}