package com.weibo.dip.pipeline.sink;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

/**
 * 对rdd结构数据的输出抽象类
 * Create by hongxun on 2018/7/26
 */
public abstract class JavaRddDataSink extends RddDataSink {

  protected RowParser parser;
//  protected Map<String,String>

  public JavaRddDataSink(Map<String, Object> params) {
    super(params);
    Map<String, Object> parserConfig = (Map<String, Object>) params.get("parser");
    parser = RowParserTypeEnum.getRowParserByMap(parserConfig);
  }

  public abstract void write(JavaRDD<Row> rdd);
}

/**
 * 写出到kafka
 */
class KafkaRddDataSink extends JavaRddDataSink {

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

/**
 * 按列值分发sink
 */
class DistributeKafkaRddDataSink extends KafkaRddDataSink {

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

/**
 * 写出到标准输入输出中
 */
class ConsoleRddDataSink extends JavaRddDataSink {

  public ConsoleRddDataSink(Map<String, Object> params) {
    super(params);
  }

  @Override
  public void write(JavaRDD<Row> rdd) {
    rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
      public void call(Iterator<Row> rowIterator) throws Exception {
        while (rowIterator.hasNext()) {
          Row row = rowIterator.next();
          System.out.println(parser.parseRow(row));
        }
      }
    });
  }

  @Override
  public void stop() {

  }
}
