package com.weibo.dip.pipeline.processor;

import com.weibo.dip.pipeline.configuration.Configuration;
import java.io.Serializable;
import java.util.Map;

/**
 * Create by hongxun on 2018/6/26
 */
public abstract class Processor extends Configuration {

  public Processor() {
  }

  public Processor(Map<String, Object> params) {
    if (params != null) {
      addConfigs(params);
    }
  }

  public abstract Map<String, Object> process(Map<String, Object> data) throws Exception;


}
