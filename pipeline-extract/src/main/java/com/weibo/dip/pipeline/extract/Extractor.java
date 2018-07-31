package com.weibo.dip.pipeline.extract;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create by hongxun on 2018/7/4
 */
public abstract class Extractor implements Serializable {

  private static final long serialVersionUID = 1L;
  protected String[] columns;

  public abstract List<Map<String, Object>> extract(String line) throws Exception;
}

class DelimiterExacter extends Extractor {

  private String split;

  public DelimiterExacter(String[] columns, String split) {
    this.columns = columns;
    this.split = split;
  }

  public DelimiterExacter(Map<String, Object> jsonMap) {
    this.columns = ((ArrayList<String>) jsonMap.get("columns")).toArray(new String[0]);
    this.split = (String) jsonMap.get("split");
  }

  @Override
  public List<Map<String, Object>> extract(String line) throws Exception {

    List<Map<String, Object>> records = null;

    String[] record = line.split(split, -1);

    if (record.length == columns.length) {

      records = new ArrayList<>();

      Map<String, Object> recordMap = new HashMap<>();

      for (int index = 0; index < columns.length; index++) {
        recordMap.put(columns[index], record[index]);
      }
      records.add(recordMap);
    }

    return records;
  }

}

class RegexExtractor extends Extractor {

  private String regex;

  private Pattern pattern;

  public RegexExtractor(Map<String, Object> jsonMap) {
    this.columns = ((ArrayList<String>) jsonMap.get("columns")).toArray(new String[0]);
    this.regex = (String) jsonMap.get("regex");
    this.pattern = Pattern.compile(regex);
  }

  public RegexExtractor(String[] columns, String regex) {
    this.columns = columns;
    this.regex = regex;
    this.pattern = Pattern.compile(regex);
  }

  @Override
  public List<Map<String, Object>> extract(String line) throws Exception {

    List<Map<String, Object>> records = null;

    Matcher matcher = pattern.matcher(line);

    if (matcher.find() && matcher.groupCount() == columns.length) {

      records = new ArrayList<Map<String, Object>>();

      Map<String, Object> recordMap = new HashMap<String, Object>();

      for (int index = 1; index <= matcher.groupCount(); index++) {
        recordMap.put(columns[index - 1], matcher.group(index));
      }

      records.add(recordMap);

    }

    return records;
  }

}

/**
 * 默认提供多个exacter的整合
 */
abstract class MultipleBaseExtractor extends Extractor {

  protected List<Extractor> exacters;

  public MultipleBaseExtractor(Map<String, Object> jsonMap) {
    exacters = new ArrayList<>();
    List<Map<String, Object>> exactersMap = (ArrayList<Map<String, Object>>) jsonMap
        .get("extractors");
    for (Map<String, Object> map : exactersMap) {
      String extractType = (String) map.get("type");
      exacters.add(ExtractorTypeEnum.getType(extractType).getExtractor(map));
    }
  }
}

class OrderMultipleExtractor extends MultipleBaseExtractor {


  public OrderMultipleExtractor(Map<String, Object> jsonMap) {
    super(jsonMap);
  }

  @Override
  public List<Map<String, Object>> extract(String line) throws Exception {
    List<Map<String, Object>> records = null;

    for (Extractor exacter :
        exacters) {
      records = exacter.extract(line);

      if (records != null) {
        break;
      }

    }
    return records;
  }
}
