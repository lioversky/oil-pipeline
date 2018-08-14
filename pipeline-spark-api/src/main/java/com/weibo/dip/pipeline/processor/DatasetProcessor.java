package com.weibo.dip.pipeline.processor;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * dataset的processor顶层类
 * Create by hongxun on 2018/7/10
 */
public abstract class DatasetProcessor extends Processor<Dataset> {

  public DatasetProcessor(Map<String, Object> params) {
    super(params);
  }
}
