package com.weibo.dip.pipeline.parse;

import com.weibo.dip.util.GsonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Row;

/**
 * row转换成json格式字符串
 */
public class JsonStrParser extends RowParser {

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
