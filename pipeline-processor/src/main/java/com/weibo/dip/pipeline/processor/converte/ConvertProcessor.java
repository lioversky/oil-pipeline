package com.weibo.dip.pipeline.processor.converte;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import java.util.Map;

/**
 * 类型转换处理器
 * Created by hongxun on 18/6/26.
 */


public class ConvertProcessor extends FieldProcessor {

  private static final long serialVersionUID = 1L;

  private Convertor convertor;

  public ConvertProcessor(Map<String, Object> params, Convertor convertor) {
    super(params);
    this.convertor = convertor;
  }

  @Override
  public Object fieldProcess(Object data) throws Exception {
    return convertor.converte(data);
  }
}
