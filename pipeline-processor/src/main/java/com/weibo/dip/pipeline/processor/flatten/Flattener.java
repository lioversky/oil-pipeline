package com.weibo.dip.pipeline.processor.flatten;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/1
 */
public abstract class Flattener extends Configuration {

  /**
   * 展开深度
   */
  private int flattenDepth = 0;
  /**
   * 是否保留父字段名称
   */
  private boolean keepParentName;
  /**
   * 保留父字段名称时的分隔符
   */
  private String joinStrIfKeepParen = ".";

  public Flattener(Map<String, Object> params) {
    keepParentName =
        !params.containsKey("keepParentName") || (boolean) params.get("keepParentName");
    if (keepParentName && params.containsKey("joinStrIfKeepParen")) {
      joinStrIfKeepParen = (String) params.get("joinStrIfKeepParen");


    }

    addConfigs(params);

  }

  public abstract Map<String, Object> flatten(Map<String, Object> data) throws Exception;

  /**
   * 递归打平值为Map 的字段，递归条件：当子值为Map且 （配置深度<1即一直递归，或当前深度小于配置深度）
   *
   * @param prefix 保留parent前缀名
   * @param object 当前处理字段的值
   * @param depth 当前深度
   * @return 所有子Map
   * @throws Exception 异常
   */
  public Map<String, Object> flattenMap(String prefix, Object object, int depth) throws Exception {
    if (object != null && object instanceof Map) {
      Map<String, Object> data = (Map<String, Object>) object;
      Map<String, Object> result = Maps.newHashMap();
      for (Map.Entry<String, Object> entry : data.entrySet()) {
        String key = keepParentName ? prefix + joinStrIfKeepParen + entry.getKey() : entry.getKey();
        //如果值为Map且未达到depth，继续递归flatten
        if (entry.getValue() instanceof Map && (flattenDepth < 1 || depth < flattenDepth)) {
          result.putAll(flattenMap(key, entry.getValue(), depth + 1));
        } else {
          result.put(key, entry.getValue());
        }
      }

      return result;
    } else {
      return Maps.newHashMap();
    }
  }
}

/**
 * 展开所有字段
 */
class AllFlattener extends Flattener {

  public AllFlattener(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Map<String, Object> flatten(Map<String, Object> data) throws Exception {
    Map<String, Object> result = Maps.newHashMap();
    for (Map.Entry<String, Object> entry : data.entrySet()) {
      result.putAll(flattenMap(entry.getKey(), entry.getValue(), 1));
    }
    return result;
  }
}

/**
 * 展开当前字段
 */
class FieldFlattener extends Flattener {

  private String fieldName;

  public FieldFlattener(Map<String, Object> params) {
    super(params);
    fieldName = (String) params.get("fieldName");
    if (Strings.isNullOrEmpty(fieldName)) {
      throw new AttrCanNotBeNullException("FieldFlattener fieldName can not be null!!!");
    }
  }

  @Override
  public Map<String, Object> flatten(Map<String, Object> data) throws Exception {
    return flattenMap(fieldName, data.get(fieldName), 1);
  }
}