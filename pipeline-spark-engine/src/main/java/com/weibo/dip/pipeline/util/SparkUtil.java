package com.weibo.dip.pipeline.util;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * dataset工具类
 * Create by hongxun on 2018/7/23
 */
public class SparkUtil {

  /**
   * 切分列，一列内容切分成多列
   *
   * @param dataset 数据集
   * @param fieldName 要切分列名称
   * @param arrName 如果保留原列时，此值与待切分列不同，否则相同
   * @param splitStr 列值分隔符
   * @param targetFields 切分后的列名数组
   * @return 增加多列的数据集
   */
  public static Dataset splitDatasetField(Dataset dataset, String fieldName, String arrName,
      String splitStr,
      String[] targetFields) {
    dataset = dataset.withColumn(arrName, functions.split(col(fieldName), splitStr));
    for (int i = 0; i < targetFields.length; i++) {
      dataset = dataset.withColumn(targetFields[i], col(arrName).getItem(i));
    }
    return dataset.drop(arrName);
  }

  /**
   * 按正则切分列，一列内容切分成多列
   *
   * @param dataset 数据集
   * @param fieldName 要切分列名称
   * @param arrName 如果保留原列时，此值与待切分列不同，否则相同
   * @param regex 正则匹配符
   * @param targetFields 切分后的列名数组
   * @return 增加多列的数据集
   */
  public static Dataset regexSplitDatasetField(Dataset dataset, String fieldName, String arrName,
      String regex,
      String[] targetFields) {
    dataset = dataset.withColumn(arrName,
        callUDF("regex_to_array", col(fieldName), lit(regex), lit(targetFields.length)))
        .where(col(arrName).isNotNull());
    for (int i = 0; i < targetFields.length; i++) {
      dataset = dataset.withColumn(targetFields[i], col(arrName).getItem(i));
    }
    return dataset.drop(arrName);
  }

  /**
   * cache文件数据
   *
   * @param spark SparkSession
   * @param tables 配置列表
   */
  public static void cache(SparkSession spark, List<Map<String, Object>> tables) throws Exception {
    for (Map<String, Object> map : tables) {
      String cacheType = (String) map.get("type");
      if ("table".equals(cacheType)) {
        table(spark, map);
      } else if ("file".equals(cacheType)) {
        file(spark, map);
      }
    }
  }

  /**
   * 依赖文件的加载与缓存
   *
   * @param map type:file
   * fileType:parquet,csv,json,txt
   * filePath:
   * split:
   * sql:
   * tableName:
   * cache :true,false
   */
  public static void file(SparkSession spark, Map<String, Object> map) throws Exception {
    String tableName = (String) map.get("tableName");
    //如果从文件中读取，映射成表

    String fileType = (String) map.get("fileType");
    String filePath = (String) map.get("filePath");
    Dataset<Row> dataset;
    DataFrameReader reader = spark.read();
    //如果非结构化的，处理成表
    if ("text".equals(fileType)) {
      //text格式加载数据
      //读取数据
      Dataset lineDataset = reader.format(fileType).load(filePath);
      String splitStr = (String) map.get("splitStr");
      Function<String, Row> func = line -> {
        String[] values = line.split(splitStr);
        return RowFactory.create(values);
      };
      //创建schema
      String[] colArr = ((String) map.get("columns")).split(",");
      List<StructField> fields = new ArrayList<>();
      for (String fieldName : colArr) {
        StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
        fields.add(field);
      }
      StructType schema = DataTypes.createStructType(fields);
      //生成dataframe
      JavaRDD<Row> rdd = lineDataset.as(Encoders.STRING()).javaRDD().map(func);
      dataset = spark.createDataFrame(rdd, schema);
    } else if ("csv".equals(fileType)) {
      //csv格式数据加载
      dataset = reader.format(fileType).load(filePath);
      String[] colArr = ((String) map.get("columns")).split(",");
      dataset = dataset.toDF(colArr);
    } else {
      dataset = reader.format(fileType).load(filePath);
    }
    //如果包含sql，做二次处理
    if (map.containsKey("sql")) {
      String sql = (String) map.get("sql");
      String tmpTableName = tableName + "_tmp_" + System.currentTimeMillis();
      dataset.createOrReplaceTempView(tmpTableName);
      sql.replace("${table}", tmpTableName);
      dataset = spark.sql(sql);
    }
    Number repartition = (Number) map.get("repartition");
    if (repartition != null) {
      dataset = dataset.repartition(repartition.intValue());
    }
    dataset.createOrReplaceTempView(tableName);

    //数据是否需要缓存
    String cache = (String) map.get("cache");
    if (cache == null || "true".equals(cache)) {
      spark.sql("cache table " + tableName);
    }

  }

  /**
   * 依赖表的加载与缓存
   *
   * @param map type:file,table
   * fileType:parquet,csv,json,txt
   * filePath:
   * split:
   * sql:
   * tableName:
   * cache :true,false
   */
  public static void table(SparkSession spark, Map<String, Object> map) throws Exception {
    String tableName = (String) map.get("tableName");
    String cache = (String) map.get("cache");
    String sql = (String) map.get("sql");
    Dataset<Row> dataset = spark.sql(sql);

    dataset.createOrReplaceTempView(tableName);
    //          数据是否需要缓存
    if (cache == null || "true".equals(cache)) {
      spark.sql("cache table " + tableName);
    }
  }


  public static StructField createStructField(String name, Object value) {
    if (value instanceof Map) {
      return DataTypes
          .createStructField(name,
              DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);
    } else if (value instanceof List) {
      return DataTypes
          .createStructField(name, DataTypes.createArrayType(DataTypes.StringType), true);
    } else {
      return DataTypes
          .createStructField(name, DataTypes.StringType, true);
    }
  }
}
