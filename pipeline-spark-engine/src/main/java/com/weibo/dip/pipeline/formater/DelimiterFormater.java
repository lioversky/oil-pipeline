package com.weibo.dip.pipeline.formater;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * row以字符分隔拼成字符串
 */
public class DelimiterFormater extends RowFormater {

  //    消息分隔符
  private String splitStr;
  private List<String> output;

  public DelimiterFormater(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
    output = (List<String>) params.get("columns");
  }

  @Override
  public Object parseRow(Row row) throws Exception {
    StringBuffer sb = new StringBuffer();
    //循环row中的每列
    if (output != null) {
      for (int i = 0; i < output.size(); i++) {
        if (i > 0) {
          sb.append(splitStr);
        }
        Object obj = row.get(row.fieldIndex(output.get(i)));
        if (obj != null) {
          sb.append(obj);
        }
      }
    } else {
      for (int i = 0; i < row.length(); i++) {
        if (i > 0) {
          sb.append(splitStr);
        }
        Object obj = row.get(i);
        if (obj != null) {
          sb.append(obj);
        }
      }
    }
    return sb.toString();
  }
}
