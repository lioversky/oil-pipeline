package com.weibo.dip.pipeline.processor.convert;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import java.util.Map;

/**
 * 类型转换处理器
 * Created by hongxun on 18/6/26.
 */


public abstract class ConvertProcessor extends FieldProcessor {

  private static final long serialVersionUID = 1L;


  public ConvertProcessor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Object fieldProcess(Object data) throws Exception {
    return converte(data);
  }


  abstract Object converte(Object data);
}
