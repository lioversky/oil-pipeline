package com.weibo.dip.pipeline.register;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

/**
 * 正则提取
 */
class RegexTableExtractor extends SplitTableExtractor {

  private String regex;
  private Pattern pattern;

  public RegexTableExtractor(Map<String, Object> params) {
    super(params);
    regex = (String) params.get("regex");
    this.pattern = Pattern.compile(regex);
    func = new FlatMapFunction<String, Row>() {
      @Override
      public Iterator<Row> call(String line) throws Exception {
        List<Row> resultList = new ArrayList<>();
        Matcher matcher = pattern.matcher(line);
        String[] values = new String[columns.length];
        if (matcher.find() && matcher.groupCount() == columns.length) {
          for (int index = 1; index <= matcher.groupCount(); index++) {
            values[index - 1] = matcher.group(index);
          }
        }
        resultList.add(RowFactory.create(values));
        return resultList.iterator();
      }
    };
  }
}
