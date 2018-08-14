package com.weibo.dip.pipeline.parse;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * 格式化Row成对应的结构抽象类.
 * Create by hongxun on 2018/8/1
 */
public abstract class RowParser implements Serializable {

  protected String[] output;

  /**
   * 构造函数
   * @param params 参数
   */
  public RowParser(Map<String, Object> params) {
    if (params.containsKey("output")) {
      output = ((List<String>) params.get("output")).toArray(new String[0]);
    }
  }

  public abstract Object parseRow(Row row) throws Exception;
}

