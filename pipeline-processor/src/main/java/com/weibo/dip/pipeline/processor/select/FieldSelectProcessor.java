package com.weibo.dip.pipeline.processor.select;

import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import com.weibo.dip.pipeline.processor.StructMapProcessor;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public abstract class FieldSelectProcessor extends StructMapProcessor {

  String[] fields;

  public FieldSelectProcessor(Map<String, Object> params) {
    super(params);
    String fields = (String) params.get("fields");
    if (StringUtils.isEmpty(fields)) {
      throw new AttrCanNotBeNullException("FieldSelector fields can not be null!!!");
    }
    this.fields = StringUtils.split(fields, ",");
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {

    //    todo: keep 时列不存在处理
    return fieldRemove(data);

  }

  abstract Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception;
}

