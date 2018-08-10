package com.weibo.dip.pipeline.register;

import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * 其它包含schema结构文件，如果parquet rcfile json
 */
public class SchemaTableExtractor extends FileTableExtractor {

  public SchemaTableExtractor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset extract(SparkSession spark) {
    return spark.read().format(fileType).load(filePath);
  }
}
