package com.weibo.dip.pipeline.processor.add;

import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.processor.Processor;
import com.weibo.dip.pipeline.processor.StructMapProcessor;
import java.util.Map;
import org.joda.time.DateTime;

/**
 * 增加新列.
 * 包括：复制列
 * 增加固定值
 * 增加各类型时间
 */
public class FieldAddProcessor extends StructMapProcessor {

  /**
   * 当目标列存在时是否覆盖
   */
  private boolean overwriteIfFieldExist;

  /**
   * 目标列名
   */
  private String targetField;
  /**
   * 处理类
   */
  private FieldAdder fieldAdder;

  public FieldAddProcessor(Map<String, Object> params,
      FieldAdder fieldAdder) {
    super(params);
    this.fieldAdder = fieldAdder;
    overwriteIfFieldExist =
        params.containsKey("overwriteIfFieldExist") && (boolean) params
            .get("overwriteIfFieldExist");
    targetField = (String) params.get("targetField");
  }


  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    //    目标列存在
    if (data.containsKey(targetField) && !overwriteIfFieldExist) {
      throw new FieldExistException(targetField);
    }
    Object result = fieldAdder.fieldAdd(data);
    if (result != null) {
      data.put(targetField, result);
    }
    return data;
  }


  @Override
  public void addConfig() {
    configs.put("overwriteIfFieldExist", overwriteIfFieldExist);
    configs.put("targetField", targetField);
  }
}


