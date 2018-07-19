package com.weibo.dip.pipeline.processor.substring;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import java.util.Map;

/**
 * 列截取处理器.
 * Create by hongxun on 2018/6/27
 */
public class SubStringProcessor extends FieldProcessor {

  private SubStringer subStringer;

  public SubStringProcessor(Map<String, Object> params, SubStringer subStringer) {
    super(params);
    this.subStringer = subStringer;
  }

  @Override
  public Object fieldProcess(Object data) throws Exception {
    return subStringer.substring((String) data);
  }

}

