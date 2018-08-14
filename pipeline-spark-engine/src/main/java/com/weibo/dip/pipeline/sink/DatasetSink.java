package com.weibo.dip.pipeline.sink;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Dataset的输出实现
 * Create by hongxun on 2018/7/26
 */

public abstract class DatasetSink extends DatasetDataSink {

  protected String sinkFormat;
  protected String sinkMode;
  protected Map<String, String> sinkOptions;
  /**
   * dataset与rdd写出相同时，调用rdd的write，不同写出时此为空
   */
  protected JavaRddDataSink rddDataSink;

  /**
   * 构造函数
   *
   * @param params 参数
   */
  public DatasetSink(Map<String, Object> params) {
    super(params);
    sinkFormat = (String) params.get("format");
    sinkMode = (String) params.get("mode");
    sinkOptions = (Map<String, String>) params.get("options");
  }
}
// todo: sink write
