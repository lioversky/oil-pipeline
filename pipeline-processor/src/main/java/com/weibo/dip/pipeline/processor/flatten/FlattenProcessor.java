package com.weibo.dip.pipeline.processor.flatten;

import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.processor.Processor;
import com.weibo.dip.pipeline.processor.StructMapProcessor;
import java.util.Map;

/**
 * 数据展开处理器
 * Create by hongxun on 2018/7/1
 */
public abstract class FlattenProcessor extends StructMapProcessor {


  private boolean overwriteIfFieldExist;
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

  public FlattenProcessor(Map<String, Object> params) {
    super(params);
    overwriteIfFieldExist =
        !params.containsKey("overwriteIfFieldExist") || (boolean) params
            .get("overwriteIfFieldExist");
    keepParentName =
        !params.containsKey("keepParentName") || (boolean) params.get("keepParentName");
    if (keepParentName && params.containsKey("joinStrIfKeepParen")) {
      joinStrIfKeepParen = (String) params.get("joinStrIfKeepParen");


    }
  }


  /**
   * @param data 原始数据
   * @return 展开后的数据
   * @throws Exception 展开后的字段已经存在异常
   */
  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    Map<String, Object> flattenMap = flatten(data);
    if (overwriteIfFieldExist) {
      data.putAll(flattenMap);
    } else {
      //如果不允许覆盖，检查
      for (Map.Entry<String, Object> entry : flattenMap.entrySet()) {
        if (data.containsKey(entry.getKey())) {
          throw new FieldExistException(entry.getKey());
        } else {
          data.put(entry.getKey(), entry.getValue());
        }
      }
    }

    return data;
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
