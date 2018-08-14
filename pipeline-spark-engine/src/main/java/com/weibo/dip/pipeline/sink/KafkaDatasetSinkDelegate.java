package com.weibo.dip.pipeline.sink;

import java.util.Map;
import org.apache.spark.sql.Dataset;

/**
 * 输出到kafka的sink
 */
public class KafkaDatasetSinkDelegate extends DatasetSink {


  public KafkaDatasetSinkDelegate(Map<String, Object> params) {
    super(params);
    String typeName = (String) params.get("format");
    rddDataSink = (JavaRddDataSink) Sink.createSink("streaming", typeName, params);
  }

  @Override
  public void write(Dataset dataset) {
    rddDataSink.write(dataset.javaRDD());
  }

  @Override
  public void stop() {
    if (rddDataSink != null) {
      rddDataSink.stop();
    }
  }
}
