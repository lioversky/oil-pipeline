package com.weibo.dip.pipeline.processor.merge;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.processor.Processor;
import com.weibo.dip.pipeline.processor.StructMapProcessor;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * 值合并处理器.
 * Create by hongxun on 2018/6/27
 */
public abstract class FieldMergeProcessor extends StructMapProcessor {


  private String targetField;
  private boolean overwriteIfFieldExist;
  protected String[] fields;

  public FieldMergeProcessor(Map<String, Object> params) {
    super(params);
    targetField = (String) params.get("targetField");
    overwriteIfFieldExist =
        !params.containsKey("overwriteIfFieldExist") || (boolean) params
            .get("overwriteIfFieldExist");
    String fields = (String) params.get("fields");
    if (Strings.isNullOrEmpty(fields)) {
      throw new AttrCanNotBeNullException("Merger fields can not be null!!!");
    }
    this.fields = StringUtils.split(fields, ",");
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    if (data.containsKey(targetField) && !overwriteIfFieldExist) {
      throw new FieldExistException(targetField);
    }

    data.put(targetField, merge(data));
    return data;
  }

  @Override
  public void addConfig() {
    configs.put("targetField", targetField);
    configs.put("overwriteIfFieldExist", overwriteIfFieldExist);
  }

  abstract Object merge(Map<String, Object> data) throws Exception;
}

