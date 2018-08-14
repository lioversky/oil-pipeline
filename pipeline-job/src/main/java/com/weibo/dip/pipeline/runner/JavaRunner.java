package com.weibo.dip.pipeline.runner;

import com.weibo.dip.pipeline.job.PipelineJob;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/2
 */
public class JavaRunner extends Runner {

  private Map<String, Object> preConfig;
  private PipelineJob job;

  public JavaRunner(Map<String, Object> params) {
    super(params);
    preConfig = (Map<String, Object>) processConfig.get("pre");
    job = new PipelineJob(preConfig);
  }

  @Override
  public void start() throws Exception {

  }

  public Map<String, Object> pre(Map<String, Object> data) throws Exception {
    return job.processJob(data);
  }


  @Override
  public void stop() throws Exception {

  }
}
