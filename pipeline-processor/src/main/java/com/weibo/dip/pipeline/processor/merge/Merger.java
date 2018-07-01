package com.weibo.dip.pipeline.processor.merge;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.weibo.dip.pipeline.configuration.Configuration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class Merger extends Configuration {

  protected String[] fields;

  public Merger(Map<String, Object> parmas) {
    String fields = (String) parmas.get("fields");
    this.fields = StringUtils.split(fields, ",");
    addConfigs(parmas);
  }

  abstract Object merge(Map<String, Object> data) throws Exception;
}

/**
 * 合成字符串
 */
class StrMerger extends Merger {

  private String splitStr;

  public StrMerger(Map<String, Object> parmas) {
    super(parmas);
    splitStr = (String) parmas.get("splitStr");

  }

  @Override
  Object merge(Map<String, Object> data) throws Exception {
    StringBuilder sb = new StringBuilder();

    for (String field : fields) {
      if (sb.length() > 0) {
        sb.append(splitStr);
      }
      if (data.containsKey(field)) {
        sb.append(data.get(field).toString());
      }

    }
    return sb.toString();
  }
}

/**
 * 合成list
 */
class ListMerger extends Merger {

  //  默认值false
  private boolean keepIfNull;

  public ListMerger(Map<String, Object> parmas) {
    super(parmas);
    keepIfNull = parmas.containsKey("keepIfNull") && (boolean) parmas.get("keepIfNull");
  }

  @Override
  Object merge(Map<String, Object> data) throws Exception {
    List<Object> list = Lists.newArrayList();
    for (String field : fields) {
      if (!data.containsKey(field) && !keepIfNull) {
        continue;
      }
      list.add(data.get(field));
    }
    return list;
  }
}

/**
 * 合成Set
 */
class SetMerger extends Merger {

  public SetMerger(Map<String, Object> parmas) {
    super(parmas);
  }

  @Override
  Object merge(Map<String, Object> data) throws Exception {
    Set<Object> set = Sets.newHashSet();
    for (String field : fields) {
      if (data.containsKey(field)) {
        set.add(data.get(field));
      }
    }
    return set;

  }
}

/**
 * 合成map
 */

class MapMerger extends Merger {

  public MapMerger(Map<String, Object> parmas) {
    super(parmas);
  }

  @Override
  Object merge(Map<String, Object> data) throws Exception {
    Map<String, Object> map = Maps.newHashMap();
    for (String field : fields) {
      if (data.containsKey(field)) {
        map.put(field, data.get(field));
      }
    }
    return map;
  }
}
