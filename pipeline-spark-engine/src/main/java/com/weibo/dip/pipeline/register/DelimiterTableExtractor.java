package com.weibo.dip.pipeline.register;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * 分隔符提取
 */
public class DelimiterTableExtractor extends SplitTableExtractor {

  private String splitStr;


  public DelimiterTableExtractor(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
    func = new FlatMapFunction<String, Row>() {
      @Override
      public Iterator<Row> call(String line) throws Exception {
        List<Row> resultList = new ArrayList<>();
        String[] values = line.split(splitStr);
        resultList.add(RowFactory.create(values));
        return resultList.iterator();
      }
    };
  }
}
