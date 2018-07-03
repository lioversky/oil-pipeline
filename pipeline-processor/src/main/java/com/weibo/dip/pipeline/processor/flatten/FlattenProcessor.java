package com.weibo.dip.pipeline.processor.flatten;

import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.processor.Processor;
import java.util.Map;

/**
 * 数据展开处理器
 * Create by hongxun on 2018/7/1
 */
public class FlattenProcessor extends Processor {


  private boolean overwriteIfFieldExist;
  private Flattener flattener;

  public FlattenProcessor(Map<String, Object> params,
      Flattener flattener) {
    super(params);
    overwriteIfFieldExist =
        !params.containsKey("overwriteIfFieldExist") || (boolean) params
            .get("overwriteIfFieldExist");
    this.flattener = flattener;
  }

  public FlattenProcessor(boolean overwriteIfFieldExist,
      Flattener flattener) {
    this.overwriteIfFieldExist = overwriteIfFieldExist;
    this.flattener = flattener;
  }

  /**
   * @param data 原始数据
   * @return 展开后的数据
   * @throws Exception 展开后的字段已经存在异常
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
