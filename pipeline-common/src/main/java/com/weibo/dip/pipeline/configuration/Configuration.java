package com.weibo.dip.pipeline.configuration;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 配置顶层类，记录配置项.
 * Create by hongxun on 2018/6/30
 */
public class Configuration implements Serializable {

  protected Map<String, Object> configs = new HashMap<>();

  public void addConfig(String key, Object value) {
    configs.put(key, value);
  }

  public void addConfig(){

  }
  public void addConfigs(Map<String, Object> config) {
    configs.putAll(config);
  }


  public Map<String, Object> getConfigs() {
    return configs;
  }

  public void setConfigs(Map<String, Object> configs) {
    this.configs = configs;
  }
}
