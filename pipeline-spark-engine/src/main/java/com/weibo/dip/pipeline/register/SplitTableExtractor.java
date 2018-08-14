package com.weibo.dip.pipeline.register;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 分隔符和正则提取的父类
 */

public class SplitTableExtractor extends FileTableExtractor {

  protected String[] columns;
  protected FlatMapFunction<String, Row> func;

  /**
   * 构造函数
   *
   * @param params 参数
   */
  public SplitTableExtractor(Map<String, Object> params) {
    super(params);
    columns = ((String) params.get("columns")).split(",");
  }

  @Override
  public Dataset extract(SparkSession spark) {
    //text格式加载数据
    //读取数据
    Dataset lineDataset = spark.read().format(fileType).load(filePath);

    //创建schema

    List<StructField> fields = new ArrayList<>();
    for (String fieldName : columns) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);
    //生成dataframe
    JavaRDD<Row> rdd = lineDataset.as(Encoders.STRING()).javaRDD().flatMap(func);
    return spark.createDataFrame(rdd, schema);
  }
}
