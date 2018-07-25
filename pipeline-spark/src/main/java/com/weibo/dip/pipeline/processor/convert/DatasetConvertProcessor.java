package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.pipeline.processor.FieldDatasetProcessor;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 转换处理器
 * Create by hongxun on 2018/7/19
 */
public class DatasetConvertProcessor extends FieldDatasetProcessor {

  private DatasetConvertor convertor;

  public DatasetConvertProcessor(Map<String, Object> params, DatasetConvertor convertor) {
    super(params);
    this.convertor = convertor;
  }

  @Override
  public Dataset fieldProcess(Dataset data) {
    return convertor.convert(fieldName,data);
  }
}
