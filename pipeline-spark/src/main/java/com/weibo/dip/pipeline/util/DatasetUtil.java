package com.weibo.dip.pipeline.util;

import static org.apache.spark.sql.functions.col;

import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Create by hongxun on 2018/7/23
 */
public class DatasetUtil {

  public static Dataset splitDataset(Dataset dataset, String fieldName, String arrName,
      String splitStr,
      String[] targetFields) {
    dataset = dataset.withColumn(arrName, functions.split(col(fieldName), splitStr));
    for (int i = 0; i < targetFields.length; i++) {
      dataset = dataset.withColumn(targetFields[i], col(arrName).getItem(i));
    }
    return dataset.drop(arrName);
  }


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
    String cache = (String) map.get("cache");
//          如果从文件中读取，映射成表

    String fileType = (String) map.get("fileType");
    String filePath = (String) map.get("filePath");
    Dataset<Row> dataset = spark.read().format(fileType).load(filePath);
//          如果非结构化的，处理成表
    if (!"parquet".equals(fileType)) {
//                todo:
    }
//          如果包含sql，做二次处理
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

//          数据是否需要缓存
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

}
