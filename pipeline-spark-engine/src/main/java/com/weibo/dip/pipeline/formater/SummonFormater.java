package com.weibo.dip.pipeline.formater;

import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * row转换成summon格式json字符串
 */
public class SummonFormater extends RowFormater {

  public SummonFormater(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object parseRow(Row row) throws Exception {
    return null;
  }
}
