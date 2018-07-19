package com.weibo.dip.pipeline.processor.replace;

import com.weibo.dip.pipeline.processor.FieldProcessor;
import java.util.Map;

/**
 * 列替换处理器.
 * Create by hongxun on 2018/6/27
 */
public class ReplaceProcessor extends FieldProcessor {


  private Replacer replacer;


  public ReplaceProcessor(Map<String,Object> params,Replacer replacer) {
    super(params);
    this.replacer = replacer;
  }

  @Override
  public Object fieldProcess(Object data) throws Exception {
    return replacer.replace((String) data);

  }

}




