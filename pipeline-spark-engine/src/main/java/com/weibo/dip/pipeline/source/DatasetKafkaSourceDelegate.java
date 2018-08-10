package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * kafka 不同版本的source代理
 * Create by hongxun on 2018/7/30
 */

public class DatasetKafkaSourceDelegate extends DatasetSource {

  /**
   * 实际的对应版本的datasetSource
   */
  private DatasetSource datasetSource;
  /**
   * kafka source生成器，当未加载到Provider时返回空
   */
  private DatasetKafkaSourceProvider kafkaSourceProvider = DatasetKafkaSourceProvider.newInstance();

  public DatasetKafkaSourceDelegate(Map<String, Object> params) {
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

  @Override
  public void stop() {
    datasetSource.stop();
  }
}

