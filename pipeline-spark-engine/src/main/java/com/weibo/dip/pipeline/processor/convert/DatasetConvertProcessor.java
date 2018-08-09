package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 转换处理器
 * Create by hongxun on 2018/7/19
 */
public abstract class DatasetConvertProcessor extends FieldDatasetProcessor {


  public DatasetConvertProcessor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset fieldProcess(Dataset data) {
    return convert(fieldName, data);
  }

  abstract Dataset convert(String fieldName, Dataset dataset);
}
