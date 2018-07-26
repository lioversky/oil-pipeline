package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.Sink;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Create by hongxun on 2018/7/26
 */
public abstract class DatasetDataSink extends Sink<Dataset> {

  public DatasetDataSink(Map<String, Object> params) {
  }

  public abstract void write(Dataset dataset);
}
