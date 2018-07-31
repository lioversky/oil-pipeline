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
  public DatasetSink(Map<String, Object> params) {
    super(params);
    sinkFormat = (String) params.get("format");
    sinkMode = (String) params.get("mode");
    sinkOptions = (Map<String, String>) params.get("options");
  }
}
// todo: sink write
/**
 * 输出到hdfs的sink
 */
class HdfsDatasetSink extends DatasetSink {

  public HdfsDatasetSink(Map<String, Object> params) {
    super(params);

  }

  @Override
  public void write(Dataset dataset) {
    dataset.write()
        .mode(sinkMode)
        .format(sinkFormat)
        .options(sinkOptions)
        .save();
  }
}

/**
 * 输出到kafka的sink
 */
class KafkaDatasetSinkDelegate extends DatasetSink {

  public KafkaDatasetSinkDelegate(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(Dataset dataset) {

  }
}

/**
 * 输出到标准输入输出的sink，
 */
class ConsoleDatasetSink extends DatasetSink {

  private boolean truncate = true;
  private Integer numRows = 20;

  public ConsoleDatasetSink(Map<String, Object> params) {
    super(params);
    if (sinkOptions != null) {
      if (sinkOptions.containsKey("truncate")) {
        truncate = Boolean.parseBoolean(sinkOptions.get("truncate"));
      }
      if (sinkOptions.containsKey("numRows")) {
        Integer.parseInt(sinkOptions.get("numRows"));
      }
    }

  }

  @Override
  public void write(Dataset dataset) {
    dataset.show(numRows, truncate);
  }
}