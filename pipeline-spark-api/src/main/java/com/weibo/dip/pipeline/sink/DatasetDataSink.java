package com.weibo.dip.pipeline.sink;

import com.weibo.dip.pipeline.Sink;
import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * Dataset输出抽象类
 * Create by hongxun on 2018/7/26
 */
public abstract class DatasetDataSink extends Sink<Dataset> {

  public DatasetDataSink(Map<String, Object> params) {
  }

  /**
   * 写出抽象方法，供子类实现
   *
   * @param dataset 数据集
   */
  public abstract void write(Dataset dataset);
}
