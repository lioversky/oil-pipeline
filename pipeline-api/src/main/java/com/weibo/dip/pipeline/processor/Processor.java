package com.weibo.dip.pipeline.processor;

import com.weibo.dip.pipeline.configuration.Configuration;
import java.util.Map;

/**
 * Create by hongxun on 2018/6/26
 */
public abstract class Processor<T> extends Configuration {

  public Processor() {
  }

  public Processor(Map<String, Object> params) {
    if (params != null) {
      addConfigs(params);
    }
  }

  public abstract  T process(T data) throws Exception;


}
