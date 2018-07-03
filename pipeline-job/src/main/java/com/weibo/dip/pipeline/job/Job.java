package com.weibo.dip.pipeline.job;

import java.io.Serializable;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/2
 */
public abstract class Job implements Serializable {

  public abstract Map<String,Object> processJob(Map<String,Object> data) throws Exception;
}
