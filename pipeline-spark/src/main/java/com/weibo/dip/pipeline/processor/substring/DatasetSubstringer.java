package com.weibo.dip.pipeline.processor.substring;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.substring_index;
import static org.apache.spark.sql.functions.trim;

import com.weibo.dip.pipeline.configuration.Configuration;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

/**
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetSubstringer extends Configuration {


  public DatasetSubstringer(Map<String, Object> params) {
  }

  abstract Dataset substring(String fieldName, Dataset dataset);
}


/**
 * 截空格
 */
class TrimDatasetSubstringer extends DatasetSubstringer {


  public TrimDatasetSubstringer(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset substring(String fieldName,Dataset dataset) {
    return dataset.withColumn(fieldName, trim(col(fieldName)));
  }
}

/**
 * 左右子串
 */
class LRDatasetSubstringer extends DatasetSubstringer {

  private Integer left;
  private Integer right;

  @Override
  public Dataset substring(String fieldName,Dataset dataset) {
    return dataset
        .withColumn(fieldName, callUDF("substring_lr", col(fieldName), lit(left), lit(right)));
  }

  public LRDatasetSubstringer(Map<String, Object> params) {
    super(params);
    this.left = params.containsKey("left") ? ((Number) params.get("left")).intValue() : 0;
    this.right = params.containsKey("right") ? ((Number) params.get("right")).intValue() : -1;
  }
}

/**
 * 定长子串
 */
class FixedDatasetSubstringer extends DatasetSubstringer {

  private int start;
  private int length;

  @Override
  public Dataset substring(String fieldName,Dataset dataset) {
    return dataset.withColumn(fieldName, functions.substring(col(fieldName), start, length));
  }

  public FixedDatasetSubstringer(Map<String, Object> params) {
    super(params);
    this.start = params.containsKey("start") ? ((Number) params.get("start")).intValue() : 0;
    this.length = params.containsKey("length") ? ((Number) params.get("length")).intValue() : -1;
  }
}

/**
 * 分隔索引子串
 */
class IndexDatasetSubstringer extends DatasetSubstringer {

  private String delim;
  private int count;

  public IndexDatasetSubstringer(Map<String, Object> params) {
    super(params);
    delim = (String) params.get("delim");
    count = (Integer) params.get("count");
  }

  @Override
  public Dataset substring(String fieldName,Dataset dataset) {
    return dataset.withColumn(fieldName, substring_index(col(fieldName), delim, count));
  }
}
