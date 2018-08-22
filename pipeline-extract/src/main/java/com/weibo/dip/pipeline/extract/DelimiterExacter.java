package com.weibo.dip.pipeline.extract;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 分隔符map提取器
 * Create by hongxun on 2018/8/9
 */
public class DelimiterExacter extends StructMapExtractor {

  private String split;


  public DelimiterExacter(Map<String, Object> jsonMap) {
    super(jsonMap);
    this.columns = ((ArrayList<String>) jsonMap.get("columns")).toArray(new String[0]);
    this.split = (String) jsonMap.get("split");
  }

  @Override
  public List<Map<String, Object>> extractLine(String line) {

    List<Map<String, Object>> records = new ArrayList<>();

    String[] record = line.split(split, -1);

    if (record.length == columns.length) {

      Map<String, Object> recordMap = Maps.newLinkedHashMapWithExpectedSize(3);
      recordMap.put("_value_", line);
      for (int index = 0; index < columns.length; index++) {
        recordMap.put(columns[index], record[index]);
      }
      records.add(recordMap);
    }

    return records;
  }

}
