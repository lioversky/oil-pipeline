package com.weibo.dip.pipeline.source;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 调用 sparkSession的 read,生成Dataset
 */
public class DatasetFileSource extends DatasetSource {

  /**
   * 输入源类型
   */
  protected String sourceFormat;
  /**
   * 源配置
   */
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

  @Override
  public void stop() {

  }
}
