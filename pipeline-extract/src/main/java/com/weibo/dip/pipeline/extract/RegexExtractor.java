package com.weibo.dip.pipeline.extract;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create by hongxun on 2018/8/9
 */
public class RegexExtractor extends StructMapExtractor {

  private String regex;

  private Pattern pattern;

  public RegexExtractor(Map<String, Object> jsonMap) {
    super(jsonMap);
    this.columns = ((ArrayList<String>) jsonMap.get("columns")).toArray(new String[0]);
    this.regex = (String) jsonMap.get("regex");
    this.pattern = Pattern.compile(regex);
  }


  @Override
  public List<Map<String, Object>> extractLine(String line) {

    List<Map<String, Object>> records = null;

    Matcher matcher = pattern.matcher(line);

    if (matcher.find() && matcher.groupCount() == columns.length) {

      records = new ArrayList<Map<String, Object>>();

      Map<String, Object> recordMap = Maps.newLinkedHashMapWithExpectedSize(3);
      recordMap.put("_value_", line);
      for (int index = 1; index <= matcher.groupCount(); index++) {
        recordMap.put(columns[index - 1], matcher.group(index));
      }

      records.add(recordMap);

    }

    return records;
  }

}
