package com.weibo.dip.pipeline.runner;

import com.google.common.collect.Lists;
import com.weibo.dip.pipeline.extract.ExactorTypeEnum;
import com.weibo.dip.pipeline.extract.Extractor;
import com.weibo.dip.pipeline.job.PipelineJob;
import com.weibo.dip.pipeline.sink.DatasetDataSink;
import com.weibo.dip.pipeline.sink.RddDataSink;
import com.weibo.dip.pipeline.source.StreamingDataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Spark Streaming的Runner
 * Create by hongxun on 2018/7/4
 */
public class SparkStreamingRunner extends Runner {

  private SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
  //  spark执行类型
  private String engineType;
  private String sourceFormat;
  private Map<String, String> sourceOptions;
  private Extractor extractor;

  private Map<String, Object> preConfig;
  private Map<String, Object> aggConfig;
  private Map<String, Object> proConfig;

  private String sinkFormat;
  private String sinkMode;
  private RddDataSink rddSink;
  private DatasetDataSink datasetSink;

  private Map<String, String> sinkOptions;

  private StreamingDataSource streamingDataSource;

  private JavaStreamingContext javaStreamingContext;

  /**
   * @param configs runner配置
   */
  @SuppressWarnings({"unchecked"})
  public SparkStreamingRunner(Map<String, Object> configs) {
    try {
      //source配置
      engineType = (String) configs.get("engineType");
      Map<String, Object> sourceConfig = (Map<String, Object>) configs.get("sourceConfig");
      sourceFormat = (String) sourceConfig.get("format");
      sourceOptions = (Map<String, String>) sourceConfig.get("options");
      Map<String, Object> extractConfig = (Map<String, Object>) sourceConfig.get("extractor");
      extractor = ExactorTypeEnum.getType(extractConfig);
      //process配置
      Map<String, Object> processConfig = (Map<String, Object>) configs.get("processConfig");
      preConfig = (Map<String, Object>) processConfig.get("pre");
      aggConfig = (Map<String, Object>) processConfig.get("agg");
      proConfig = (Map<String, Object>) processConfig.get("pro");
      Map<String, Object> sinkConfig = (Map<String, Object>) configs.get("sinkConfig");
      sinkFormat = (String) sinkConfig.get("format");
      sinkMode = (String) sinkConfig.get("mode");
      sinkOptions = (Map<String, String>) sinkConfig.get("options");

      String checkpointDirectory = (String) configs.get("checkpointDirectory");
      if (checkpointDirectory == null) {
        javaStreamingContext = createContext(configs);
      } else {
        Function0<JavaStreamingContext> createContextFunc =
            () -> createContext(configs);
        javaStreamingContext = JavaStreamingContext
            .getOrCreate(checkpointDirectory, createContextFunc);
      }
    } catch (Exception e) {
      throw new RuntimeException("Create SparkStreamingRunner Error !!!", e);
    }

  }


  public void start() throws Exception {

    JavaDStream sourceDstream = streamingDataSource.createSource(javaStreamingContext);
    JavaDStream<Row> processDstream = process(sourceDstream);
    if (processDstream != null) {
      write(processDstream);
    }
    javaStreamingContext.start();
    javaStreamingContext.awaitTermination();
  }

  /**
   * streaming处理流程：
   * pre可有可无；
   * 如果存在agg，会调用foreachRDD生成Dataset，
   */
  private JavaDStream<Row> process(JavaDStream dstream) {
    if (preConfig != null) {
      dstream = pre(dstream);
    }
    if (aggConfig != null) {
      agg(dstream);
      return null;
    } else {
      return dstream;
    }

  }

  private void agg(JavaDStream dstream) {
    List<StructField> fields = new ArrayList<>();
    String[] columns = new String[0];
    for (String fieldName : columns) {
      StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
      fields.add(field);
    }
    StructType schema = DataTypes.createStructType(fields);
    ((JavaDStream<Row>) dstream).foreachRDD(rdd -> {
      SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
      spark.createDataFrame(rdd, schema).createOrReplaceTempView("");
      Dataset dataset = spark.sql("");
      if (proConfig != null) {
        dataset = pro(dataset);
      }
      write(dataset);
    });

  }

  private Dataset pro(Dataset dataset) {

    return dataset;
  }

  /**
   * 前阶处理
   *
   * @param dstream 原始数据集
   * @return 处理后dataset
   */
  private JavaDStream<Row> pre(JavaDStream dstream) {

    String[] columns = ((List<String>) preConfig.get("output")).toArray(new String[0]);
    PipelineJob job = new PipelineJob(preConfig);

    FlatMapFunction<String, Row> processFunction = x -> {
      List<Row> rows = Lists.newArrayList();
      List<Map<String, Object>> extractList = extractor.extract(x);
      for (Map<String, Object> data : extractList) {
        Object[] values = new Object[columns.length];
        data = job.processJob(data);
        if (data != null) {
          for (int i = 0; i < columns.length; i++) {
            values[i] = data.get(columns[i]);
          }
        }
        rows.add(RowFactory.create(values));
      }

      return rows.iterator();
    };
    return dstream.flatMap(processFunction);
  }

  private void write(Dataset dataset) {
    datasetSink.write(dataset);
  }

  private void write(JavaDStream dstream) {
    ((JavaDStream<Row>) dstream).foreachRDD(rdd -> rddSink.write(rdd));
  }

  private JavaStreamingContext createContext(Map<String, Object> jsonMap) throws Exception {
    SparkConf conf = new SparkConf();
    String appName = (String) jsonMap.get("name");
    conf.setAppName(appName);

    final JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf,
        Durations.milliseconds(((Number) jsonMap.get("duration")).longValue()));
    String checkpointDirectory = (String) jsonMap.get("checkpointDirectory");
    if (checkpointDirectory != null) {
      javaStreamingContext.checkpoint(checkpointDirectory);
    }
    return javaStreamingContext;

  }


  @Override
  public void stop() throws Exception {
    javaStreamingContext.stop(true, true);
  }
}
