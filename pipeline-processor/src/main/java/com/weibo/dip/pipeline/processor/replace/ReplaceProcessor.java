package com.weibo.dip.pipeline.processor.replace;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import java.util.Map;

/**
 * 列替换处理器.
 * Create by hongxun on 2018/6/27
 */
public abstract class ReplaceProcessor extends FieldProcessor {


  public ReplaceProcessor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object fieldProcess(Object data) throws Exception {
    return replace((String) data);

  }

  abstract Object replace(String value) throws Exception;
}




