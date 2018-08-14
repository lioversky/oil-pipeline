package com.weibo.dip.pipeline.register;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * csv格式内容提取器
 */
public class CsvTableExtractor extends FileTableExtractor {

  /**
   * 字段列名
   */

  private String[] columns;
  /**
   * 是否包含表头
   */

  private String header;

  public CsvTableExtractor(Map<String, Object> params) {
    super(params);
    this.columns = ((String) params.get("columns")).split(",");
    this.header = (String) params.get("header");
  }

  @Override
  public Dataset extract(SparkSession spark) {
    //判断是否含表头
    if (StringUtils.isNotEmpty(header) && "true".equals(header)) {
      return spark.read().format(fileType).option("header", "true").load(filePath);
    } else {
      return spark.read().format(fileType).load(filePath).toDF(columns);
    }
  }
}
