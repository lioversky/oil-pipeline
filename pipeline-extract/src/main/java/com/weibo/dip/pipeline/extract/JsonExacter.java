package com.weibo.dip.pipeline.extract;

import com.weibo.dip.util.GsonUtil;
import com.weibo.dip.util.GsonUtil.GsonType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * json map提取器
 * Create by hongxun on 2018/8/14
 */
public class JsonExacter extends StructMapExtractor {

  private Boolean isArray;

  public JsonExacter(Map<String, Object> params) {
    super(params);
    if (params.get("isArray") == null) {
      isArray = false;
    } else {
      isArray = (Boolean) params.get("isArray");
    }
  }

  @Override
  public List<Map<String, Object>> extractLine(String line) {
    List<Map<String, Object>> records = new ArrayList<>();
    try {
      if (isArray) {
        List<Object> jsonArray = GsonUtil.fromJson(line, GsonType.OBJECT_LIST_TYPE);
        if (jsonArray == null) {
          return records;
        }

        for (Object recordObj : jsonArray) {
          @SuppressWarnings("unchecked")
          Map<String, Object> json = (Map<String, Object>) recordObj;
          Map<String, Object> mapRecord = filterByColumn(json);
          records.add(mapRecord);
        }

      } else {
        Map<String, Object> json = GsonUtil.fromJson(line, GsonType.OBJECT_MAP_TYPE);

        if (json == null) {
          return records;
        }
        Map<String, Object> mapRecord = filterByColumn(json);
        records.add(mapRecord);

      }
    } catch (Exception e) {

    }
    return records;
  }


  private Map<String, Object> filterByColumn(Map<String, Object> json) {

    Map<String, Object> mapRecord = new HashMap<String, Object>();

    for (int index = 0; index < columns.length; index++) {

      Object value = json.get(columns[index]);
      if (value != null) {
        mapRecord.put(columns[index], value);
      } else {
        mapRecord.put(columns[index], null);
      }
    }
    return mapRecord;
  }

}
