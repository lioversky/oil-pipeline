package com.weibo.dip.pipeline.sink;

import java.util.Map;
import org.apache.spark.sql.Dataset;

public abstract class DatasetSink extends DatasetDataSink {

  protected String sinkFormat;
  protected String sinkMode;
  protected Map<String, String> sinkOptions;

  public DatasetSink(Map<String, Object> params) {
    super(params);
    sinkFormat = (String) params.get("format");
    sinkMode = (String) params.get("mode");
    sinkOptions = (Map<String, String>) params.get("options");
  }
}

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

class KafkaDatasetSink extends DatasetSink {

  public KafkaDatasetSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(Dataset dataset) {

  }
}

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