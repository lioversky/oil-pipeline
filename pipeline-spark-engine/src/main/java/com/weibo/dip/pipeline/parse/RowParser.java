package com.weibo.dip.pipeline.parse;

import com.weibo.dip.util.GsonUtil;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * 格式化Row成对应的结构抽象类.
 * Create by hongxun on 2018/8/1
 */
public abstract class RowParser implements Serializable {

  protected String[] output;

  public RowParser(Map<String, Object> params) {
    if (params.containsKey("output")) {
      output = ((List<String>) params.get("output")).toArray(new String[0]);
    }
  }

  public abstract Object parseRow(Row row) throws Exception;
}

/**
 * row转换成json格式字符串
 */
class JsonStrParser extends RowParser {

  public JsonStrParser(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object parseRow(Row row) throws Exception {
    Map<String, Object> map = new HashMap<>();
    if (output != null) {
      for (int i = 0; i < output.length; i++) {
        map.put(output[i], row.get(i));
      }
    } else {
      for (int i = 0; i < row.length(); i++) {
        map.putAll(row.getJavaMap(i));
      }
    }
    return GsonUtil.toJson(map);
  }
}

/**
 * row以字符分隔拼成字符串
 */
class DelimiterParser extends RowParser {

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

/**
 * row转换成summon格式json字符串
 */
class SummonParser extends RowParser {

  public SummonParser(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object parseRow(Row row) throws Exception {
    return null;
  }
}