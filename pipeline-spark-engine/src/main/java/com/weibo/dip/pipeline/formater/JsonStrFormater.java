package com.weibo.dip.pipeline.formater;

import com.weibo.dip.util.GsonUtil;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * row转换成json格式字符串
 */
public class JsonStrFormater extends RowFormater {

  public JsonStrFormater(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object parseRow(Row row) throws Exception {
    Map<String, Object> map = new HashMap<>();

    StructType schema = row.schema();
    String[] fields = schema.fieldNames();
    for (int i = 0; i < row.length(); i++) {
      map.put(fields[i], row.get(i));
    }

    return GsonUtil.toJson(map);
  }
}
