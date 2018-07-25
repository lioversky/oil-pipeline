package com.weibo.dip.pipeline.extract;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.DataFrameReader;
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
public abstract class FielTableExtractor implements Serializable {

  protected String fileType;
  protected String tableName;
  protected String filePath;
  protected String cache;


  protected DataFrameReader reader;

  public FielTableExtractor(Map<String, Object> params) {
    this.tableName = (String) params.get("tableName");
    //如果从文件中读取，映射成表

    this.fileType = (String) params.get("fileType");
    this.filePath = (String) params.get("filePath");
    this.cache = (String) params.get("cache");
  }

  public void cacheTable(SparkSession spark, Map<String, Object> map) {

    reader = spark.read();
    Dataset dataset = extract(spark);
    //如果包含sql，做二次处理
    //    if (map.containsKey("sql")) {
    //      String tmpTableName = tableName + "_tmp_" + System.currentTimeMillis();
    //      dataset.createOrReplaceTempView(tmpTableName);
    //      sql.replace("${table}", tmpTableName);
    //      dataset = spark.sql(sql);
    //    }
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

  public abstract Dataset extract(SparkSession spark);
}

/**
 * csv格式内容提取器
 */
class CsvTableExtractor extends FielTableExtractor {

  /**
   * 字段列名
   */

  private String[] columns;
  /**
   * 是否包含表头
   */

  private String header;

  public CsvTableExtractor(Map<String, Object> params) {
    super(params);
    this.columns = ((String) params.get("columns")).split(",");
    this.header = (String) params.get("header");
  }

  @Override
  public Dataset extract(SparkSession spark) {
    //判断是否含表头
    if (StringUtils.isNotEmpty(header) && "true".equals(header)) {
      return reader.format(fileType).option("header", "true").load(filePath);
    } else {
      return reader.format(fileType).load(filePath).toDF(columns);
    }
  }
}


/**
 * 分隔符和正则提取的父类
 */
class SplitTableExtractor extends FielTableExtractor {

  protected String[] columns;
  protected FlatMapFunction<String, Row> func;

  public SplitTableExtractor(Map<String, Object> params) {
    super(params);
    columns = ((String) params.get("columns")).split(",");
  }

  @Override
  public Dataset extract(SparkSession spark) {
    //text格式加载数据
    //读取数据
    Dataset lineDataset = reader.format(fileType).load(filePath);

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

/**
 * 分隔符提取
 */
class DelimiterTableExtractor extends SplitTableExtractor {

  private String splitStr;


  public DelimiterTableExtractor(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
    func = line -> {
      List<Row> resultList = new ArrayList<>();
      String[] values = line.split(splitStr);
      resultList.add(RowFactory.create(values));
      return resultList.iterator();
    };
  }
}

/**
 * 正则提取
 */
class RegexTableExtractor extends SplitTableExtractor {

  private String regex;
  private Pattern pattern;

  public RegexTableExtractor(Map<String, Object> params) {
    super(params);
    regex = (String) params.get("regex");
    this.pattern = Pattern.compile(regex);
    func = line -> {
      List<Row> resultList = new ArrayList<>();
      Matcher matcher = pattern.matcher(line);
      String[] values = new String[columns.length];
      if (matcher.find() && matcher.groupCount() == columns.length) {
        for (int index = 1; index <= matcher.groupCount(); index++) {
          values[index - 1] = matcher.group(index);
        }
      }
      resultList.add(RowFactory.create(values));
      return resultList.iterator();
    };
  }
}

/**
 * 已存在spark或者hive表，执行sql
 */
class SparkTableExtractor extends FielTableExtractor {

  private String sql;

  public SparkTableExtractor(Map<String, Object> params) {
    super(params);
    this.sql = (String) params.get("sql");
  }

  public Dataset extract(SparkSession spark) {
    return spark.sql(sql);
  }
}

/**
 * 其它包含schema结构文件，如果parquet rcfile json
 */
class SchmaTableExtractor extends FielTableExtractor {

  public SchmaTableExtractor(Map<String, Object> params) {
    super(params);
  }

  @Override
  public Dataset extract(SparkSession spark) {
    return reader.format(fileType).load(filePath);
  }
}