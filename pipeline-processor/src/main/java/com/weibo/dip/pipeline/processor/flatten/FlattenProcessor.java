package com.weibo.dip.pipeline.processor.flatten;

import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.processor.Processor;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/1
 */
public class FlattenProcessor extends Processor {

  private Flattener flattener;

  private boolean overwriteIfFieldExist;

  /**
   * @param data 原始数据
   * @return 打平后的数据
   * @throws Exception 打平后的字段已经存在异常
   */
  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    Map<String, Object> flattenMap = flattener.flatten(data);
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
}
