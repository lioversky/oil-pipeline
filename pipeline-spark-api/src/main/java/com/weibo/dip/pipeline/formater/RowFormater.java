package com.weibo.dip.pipeline.formater;

import com.weibo.dip.pipeline.format.Formater;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * 格式化Row成对应的结构抽象类.
 * Create by hongxun on 2018/8/1
 */
public abstract class RowFormater extends Formater<Row> {


  /**
   * 构造函数
   * @param params 参数
   */
  public RowFormater(Map<String, Object> params) {
    super(params);
  }

}

