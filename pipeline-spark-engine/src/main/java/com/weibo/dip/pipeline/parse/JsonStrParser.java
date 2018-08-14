package com.weibo.dip.pipeline.parse;

import com.weibo.dip.util.GsonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

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
      StructType schema = row.schema();
      String[] fields = schema.fieldNames();
      for (int i = 0; i < row.length(); i++) {
        map.put(fields[i], row.get(i));
      }
    }
    return GsonUtil.toJson(map);
  }
}
