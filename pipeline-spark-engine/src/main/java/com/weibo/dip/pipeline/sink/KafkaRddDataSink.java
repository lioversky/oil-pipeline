package com.weibo.dip.pipeline.sink;

import java.util.Iterator;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

/**
 * 写出到kafka
 */
public class KafkaRddDataSink extends JavaRddDataSink {

  protected KafkaDataSink kafkaDataSink;
  private KafkaSinkProvider provider = KafkaSinkProvider.newInstance();

  public KafkaRddDataSink(Map<String, Object> params) {
    super(params);
    kafkaDataSink = provider.createDataSink(params);
  }

  @Override
  public void write(JavaRDD<Row> rdd) {
    rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
      public void call(Iterator<Row> rowIterator) throws Exception {
        try {
          while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            kafkaDataSink.write((String) parser.parseRow(row));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  @Override
  public void stop() {
    kafkaDataSink.stop();
  }
}
