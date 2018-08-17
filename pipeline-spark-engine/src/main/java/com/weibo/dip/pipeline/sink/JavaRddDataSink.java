package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.formater.RowFormater;
import com.weibo.dip.pipeline.formater.RowFormaterTypeEnum;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

/**
 * 对rdd结构数据的输出抽象类
 * Create by hongxun on 2018/7/26
 */
public abstract class JavaRddDataSink extends RddDataSink {

  protected RowFormater parser;
//  protected Map<String,String>

  public JavaRddDataSink(Map<String, Object> params) {
    super(params);
    Map<String, Object> parserConfig = (Map<String, Object>) params.get("parser");
    parser = RowFormaterTypeEnum.getRowParserByMap(parserConfig);
  }

  public abstract void write(JavaRDD<Row> rdd);
}

