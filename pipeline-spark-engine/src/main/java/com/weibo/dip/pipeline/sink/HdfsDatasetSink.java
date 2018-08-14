package com.weibo.dip.pipeline.sink;

import java.util.Map;
import org.apache.spark.sql.Dataset;

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

  @Override
  public void stop() {

  }
}
