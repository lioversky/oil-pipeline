package com.weibo.dip.pipeline.source;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

/**
 * 离线按照topic and offset读取kafka数据
 * Create by hongxun on 2018/7/27
 */
public class DatasetKafka010DataSource extends DatasetSource {

  /**
   * kafka配置
   */
  private Map<String, Object> kafkaParams;
  /**
   * topic and offset定义，fromOffset，untilOffset
   */
  private OffsetRange[] offsetRanges;

  public DatasetKafka010DataSource(Map<String, Object> params) {
    super(params);
    kafkaParams = (Map<String, Object>) params.get("options");
    if (!params.containsKey("offsetRanges")) {
      throw new IllegalArgumentException("No offsetRanges config !!!");
    }
    List<Map<String, Object>> offsetRangeList = (List<Map<String, Object>>) params
        .get("offsetRanges");
    //生成topic and offset位置
    offsetRanges = new OffsetRange[offsetRangeList.size()];
    for (int i = 0; i < offsetRangeList.size(); i++) {
      Map<String, Object> offsetRangeMap = offsetRangeList.get(i);
      String topic = (String) offsetRangeMap.get("topic");
      int partition = ((Number) offsetRangeMap.get("partition")).intValue();
      Long fromOffset = ((Number) offsetRangeMap.get("fromOffset")).longValue();
      Long untilOffset = ((Number) offsetRangeMap.get("untilOffset")).longValue();

      offsetRanges[i] = OffsetRange.create(topic, partition, fromOffset, untilOffset);
    }

  }

  @Override
  public Dataset createSource(SparkSession sparkSession) {

    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
    //调用方法生成rdd
    JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
        javaSparkContext,
        kafkaParams,
        offsetRanges,
        LocationStrategies.PreferConsistent()
    );
    JavaRDD<Row> stringJavaRDD = rdd.map(record -> RowFactory.create(record.value()));

    List<StructField> fields = Arrays
        .asList(DataTypes.createStructField("_value_", DataTypes.StringType, true));

    StructType schema = DataTypes.createStructType(fields);
    //返回dataset
    return sparkSession.createDataFrame(stringJavaRDD, schema);
  }
}
