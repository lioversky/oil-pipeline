package com.weibo.dip.pipeline.processor.merge;

import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.processor.Processor;
import java.util.Map;

/**
 * 值合并处理器.
 * Create by hongxun on 2018/6/27
 */
public class FieldMergeProcessor extends Processor {


  private String targetField;
  private boolean overwriteIfFieldExist;
  private Merger merger;

  public FieldMergeProcessor(Map<String, Object> params,
      Merger merger) {
    super(params);
    this.merger = merger;
    targetField = (String) params.get("targetField");
    overwriteIfFieldExist =
        !params.containsKey("overwriteIfFieldExist") || (boolean) params
            .get("overwriteIfFieldExist");
  }

  public FieldMergeProcessor(String targetField, boolean overwriteIfFieldExist,
      Merger merger) {
    this.targetField = targetField;
    this.overwriteIfFieldExist = overwriteIfFieldExist;
    this.merger = merger;
    addConfig();
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    if (data.containsKey(targetField) && !overwriteIfFieldExist) {
      throw new FieldExistException(targetField);
    }

    data.put(targetField, merger.merge(data));
    return data;
  }

  @Override
  public void addConfig() {
    configs.put("targetField", targetField);
    configs.put("overwriteIfFieldExist", overwriteIfFieldExist);
  }
}

