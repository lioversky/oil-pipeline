package com.weibo.dip.pipeline.parse;

import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * row以字符分隔拼成字符串
 */
public class DelimiterParser extends RowParser {

  //    消息分隔符
  private String splitStr;

  public DelimiterParser(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
  }

  @Override
  public Object parseRow(Row row) throws Exception {
    StringBuffer sb = new StringBuffer();
    //循环row中的每列
    for (int i = 0; i < row.length(); i++) {
      if (i > 0) {
        sb.append(splitStr);
      }
      Object obj = row.get(i);
      if (obj != null) {
        sb.append(obj);
      }
    }
    return sb.toString();
  }
}
