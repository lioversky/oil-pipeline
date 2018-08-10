package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.parse.RowParser;
import com.weibo.dip.pipeline.parse.RowParserTypeEnum;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

/**
 * 对rdd结构数据的输出抽象类
 * Create by hongxun on 2018/7/26
 */
public abstract class JavaRddDataSink extends RddDataSink {

  protected RowParser parser;
//  protected Map<String,String>

  public JavaRddDataSink(Map<String, Object> params) {
    super(params);
    Map<String, Object> parserConfig = (Map<String, Object>) params.get("parser");
    parser = RowParserTypeEnum.getRowParserByMap(parserConfig);
  }

  public abstract void write(JavaRDD<Row> rdd);
}

