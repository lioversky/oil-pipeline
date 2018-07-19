package com.weibo.dip.pipeline.processor;

import java.util.Map;

/**
 * Create by hongxun on 2018/7/10
 */
public abstract class StructMapProcessor extends Processor<Map<String, Object>> {

  public StructMapProcessor(Map<String, Object> params) {
    super(params);
  }

}
