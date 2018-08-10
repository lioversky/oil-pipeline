package com.weibo.dip.pipeline.register;

import com.weibo.dip.pipeline.extract.TableExtractor;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * 文件内容提取器
 * Create by hongxun on 2018/7/25
 */
public abstract class FileTableExtractor extends TableExtractor {

  protected String fileType;
  protected String tableName;
  protected String filePath;
  protected String cache;
  protected Number repartition;


  public FileTableExtractor(Map<String, Object> params) {
    super(params);
    this.tableName = (String) params.get("tableName");
    //如果从文件中读取，映射成表

    this.fileType = (String) params.get("fileType");
    this.filePath = (String) params.get("filePath");
    this.cache = (String) params.get("cache");
    this.repartition = (Number) params.get("repartition");
  }

  public void cacheTable(SparkSession spark) {

    Dataset dataset = extract(spark);
    //如果包含sql，做二次处理
    //    if (map.containsKey("sql")) {
    //      String tmpTableName = tableName + "_tmp_" + System.currentTimeMillis();
    //      dataset.createOrReplaceTempView(tmpTableName);
    //      sql.replace("${table}", tmpTableName);
    //      dataset = spark.sql(sql);
    //    }

    if (repartition != null) {
      dataset = dataset.repartition(repartition.intValue());
    }
    dataset.createOrReplaceTempView(tableName);

    //数据是否需要缓存
    if (cache == null || "true".equals(cache)) {
      spark.sql("cache table " + tableName);
    }
  }

  public static void cacheTable(SparkSession spark, List<Map<String, Object>> tables)
      throws Exception {
    FileTableExtractor extractor = null;
    for (Map<String, Object> map : tables) {
      String cacheType = (String) map.get("type");
      if ("table".equals(cacheType)) {
        extractor = FileTableExtractorTypeEnum.getType("table", map);
      } else if ("file".equals(cacheType)) {
        String fileType = (String) map.get("fileType");
        extractor = FileTableExtractorTypeEnum.getType(fileType, map);
      }
      extractor.cacheTable(spark);
    }
  }

}


