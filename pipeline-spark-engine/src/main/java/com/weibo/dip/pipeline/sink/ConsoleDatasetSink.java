package com.weibo.dip.pipeline.sink;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 输出到标准输入输出的sink，
 */
public class ConsoleDatasetSink extends DatasetSink {

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
    String typeName = (String) params.get("type");
//    rddDataSink = (JavaRddDataSink) Sink.createSink("streaming", typeName, params);
  }

  @Override
  public void write(Dataset dataset) {
    dataset.show(numRows, truncate);
//    rddDataSink.write(dataset.javaRDD());
  }

  @Override
  public void stop() {
    if (rddDataSink != null) {
      rddDataSink.stop();
    }
  }
}
