package com.weibo.dip.pipeline.sink;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

/**
 * 按列值分发sink
 */
public class DistributeKafkaRddDataSink extends KafkaRddDataSink {

  /**
   * 要分发的列名称
   */
  private String distributeCol;
  /**
   * 分发值与topic对应关系
   */
  private Map<String, String> distributeMapping;
  /**
   * 分发列在row中的位置
   */
  private int colIndex = -1;

  public DistributeKafkaRddDataSink(Map<String, Object> params) {
    super(params);
    distributeCol = (String) params.get("distributeCol");
    distributeMapping = (Map<String, String>) params.get("distributeMapping");
    //如果包含output输出列名，取对应位置
    if (params.containsKey("output")) {
      List<String> output = ((List<String>) params.get("output"));
      colIndex = output.indexOf(distributeCol);
    }
  }

  @Override
  public void write(JavaRDD<Row> rdd) {
    rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
      public void call(Iterator<Row> rowIterator) throws Exception {
        try {
          while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            //表明未定义输出列名，则表示row中带有schema，从中取位置
            if (colIndex < 0) {
              colIndex = row.fieldIndex(distributeCol);
            }
            Object value = row.get(colIndex);
            //获取要分发到的topic
            String topic = distributeMapping.get(value);
            kafkaDataSink.write(topic, (String) parser.parseRow(row));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }
}
