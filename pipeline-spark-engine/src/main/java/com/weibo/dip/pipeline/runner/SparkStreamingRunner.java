package com.weibo.dip.pipeline.runner;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.extract.Extractor;
import com.weibo.dip.pipeline.extract.StructMapExtractor;
import com.weibo.dip.pipeline.job.PipelineJob;
import com.weibo.dip.pipeline.register.FileTableExtractor;
import com.weibo.dip.pipeline.sink.DatasetDataSink;
import com.weibo.dip.pipeline.sink.RddDataSink;
import com.weibo.dip.pipeline.sink.Sink;
import com.weibo.dip.pipeline.source.Source;
import com.weibo.dip.pipeline.source.StreamingDataSource;
import com.weibo.dip.pipeline.udf.UDFRegister;
import com.weibo.dip.pipeline.util.SparkUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Spark Streaming的Runner
 *
 * Create by hongxun on 2018/7/4
 */
public class SparkStreamingRunner extends Runner {

  protected SparkSession sparkSession;
  private String engine = "streaming";

  private String sourceFormat;
  private Map<String, String> sourceOptions;
  protected List<Map<String, Object>> tables;

  private StructMapExtractor extractor;

  private Map<String, Object> preConfig;
  private PipelineJob pipelineJob;
  /**
   * pre阶段的输出列，可以为空
   */
//  private List<String> preOutputColumns;
  private Map<String, Object> aggConfig;
  private Map<String, Object> proConfig;

  private String sinkFormat;

  private Map<String, String> sinkOptions;

  private StreamingDataSource streamingDataSource;

  private transient JavaStreamingContext javaStreamingContext;

  /**
   * @param configs runner配置
   */
  @SuppressWarnings({"unchecked"})
  public SparkStreamingRunner(Map<String, Object> configs) {
    super(configs);
    try {
      //source配置
      sourceFormat = (String) sourceConfig.get("format");
      sourceOptions = (Map<String, String>) sourceConfig.get("options");
      tables = (List<Map<String, Object>>) sourceConfig.get("tables");
      streamingDataSource = (StreamingDataSource) Source
          .createSource(engine, sourceFormat, sourceConfig);

      Map<String, Object> extractConfig = (Map<String, Object>) sourceConfig.get("extractor");
      if (extractConfig != null) {
        extractor = (StructMapExtractor) Extractor.createExtractor(engine, extractConfig);
      }
      //process配置，processConfig可以为空，为空则没有数据处理
      if (processConfig != null) {
        preConfig = (Map<String, Object>) processConfig.get("pre");
        if (preConfig != null) {
          pipelineJob = new PipelineJob(preConfig);
        }
        aggConfig = (Map<String, Object>) processConfig.get("agg");
        proConfig = (Map<String, Object>) processConfig.get("pro");
      }

      //sink配置
      sinkFormat = (String) sinkConfig.get("format");
      //sinkMode = (String) sinkConfig.get("mode");
      //sinkOptions = (Map<String, String>) sinkConfig.get("options");

    } catch (Exception e) {
      throw new RuntimeException("Create SparkStreamingRunner Error .", e);
    }

  }

  /**
   * runner的start方法实现.
   * 创建StreamingContext实例；注册udf；cache table；创建源Dstream；process；output；
   */
  public void start() throws Exception {
    sparkSession = SparkSession.builder().master("local[*]")
        .config("spark.streaming.kafka.maxRatePerPartition", 100).getOrCreate();
    String checkpointDirectory = (String) applicationConfig.get("checkpointDirectory");
    if (checkpointDirectory == null) {
      javaStreamingContext = createContext(applicationConfig);
    } else {
      Function0<JavaStreamingContext> createContextFunc =
          () -> createContext(applicationConfig);
      javaStreamingContext = JavaStreamingContext
          .getOrCreate(checkpointDirectory, createContextFunc);
    }

    //其它依赖数据源
    if (tables != null) {
      FileTableExtractor.cacheTable(sparkSession, tables);
    }
    //如果包含agg，注册udf
    if (aggConfig != null) {
      UDFRegister.registerAllUDF(sparkSession);
    }
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

    dstream = pre(dstream);
    if (aggConfig != null) {
      agg(dstream);
      return null;
    } else {
      return dstream;
    }

  }

  /**
   * 聚合
   *
   * @param dstream stream
   */
  private void agg(JavaDStream dstream) {

    if (aggConfig.containsKey("tempTableName")) {
      //取dataset引擎
      DatasetDataSink dataSink = (DatasetDataSink) Sink
          .createSink("dataset", sinkFormat, sinkConfig);
      //注册临时表名
      String tempTableName = (String) aggConfig.get("tempTableName");
      String sql = (String) aggConfig.get("sql");

      ((JavaDStream<Row>) dstream).foreachRDD(rdd -> {
        //如果配置output，使用创建schema，否则使用row的schema
        StructType schema;

        schema = rdd.first().schema();

        SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
        spark.createDataFrame(rdd, schema).createOrReplaceTempView(tempTableName);
        Dataset dataset = spark.sql(sql);
        if (proConfig != null) {
          dataset = pro(dataset);
        }
        write(dataset, dataSink);
      });
    }

  }

  /**
   * 后阶处理
   * @param dataset 聚合后的数据集
   * @return 处理后的数据集
   * @throws Exception 异常
   */
  private Dataset pro(Dataset dataset) throws Exception {

    List<Map<String, Object>> stagesConfigList = (List<Map<String, Object>>) proConfig
        .get("stages");
    if (stagesConfigList != null && !stagesConfigList.isEmpty()) {
      return DatasetRunner.processStage(dataset, stagesConfigList);
    }
    return dataset;
  }

  /**
   * 前阶处理
   * Extractor可以为空
   * pipelineJob可以为空
   * @param dstream 原始数据集
   * @return 处理后dataset
   */
  private JavaDStream<Row> pre(JavaDStream dstream) {

//    preOutputColumns = ((List<String>) preConfig.get("output"));

    StructMapExtractor sourceExtractor = extractor;
    FlatMapFunction<String, Row> processFunction = new FlatMapFunction<String, Row>() {
      @Override
      public Iterator<Row> call(String s) throws Exception {
        List<Row> rows = Lists.newArrayList();
        //抽取数据
        List<Map<String, Object>> extractResult;
        if (sourceExtractor != null) {
          extractResult = sourceExtractor.extract(s);
        } else {
          extractResult = Lists.newArrayList(Maps.newHashMap(ImmutableMap.of("_value_", s)));
        }
        //遍历数据

        for (Map<String, Object> data : extractResult) {
          //如果pipelineJob为空，则没有数据处理，直接进入sink
          if (pipelineJob != null) {
            data = pipelineJob.processJob(data);
          }
          //将dataMap转换成row
          if (data != null) {
            Object[] values;
            List<StructField> fields = new ArrayList<>();

            //未配置output，除了原始数据全部输出
            data.remove("_value_");
            List<Object> objects = new ArrayList<>();
            for (Map.Entry<String, Object> entry : data.entrySet()) {
              objects.add(entry.getValue());
              fields.add(SparkUtil.createStructField(entry.getKey(), entry.getValue()));
            }
            values = objects.toArray(new Object[0]);

            StructType schema = DataTypes.createStructType(fields);
            rows.add(new GenericRowWithSchema(values, schema));
          }

        }

        return rows.iterator();
      }
    };
    return dstream.flatMap(processFunction);
  }


  private void write(Dataset dataset, DatasetDataSink dataSink) {
    dataSink.write(dataset);
  }

  private void write(JavaDStream dstream) {
    RddDataSink dataSink = (RddDataSink) Sink.createSink(engine, sinkFormat, sinkConfig);
    ((JavaDStream<Row>) dstream).foreachRDD(rdd -> dataSink.write(rdd));
  }

  /**
   * 创建JavaStreamingContext
   *
   * @param jsonMap 应用配置
   */
  private JavaStreamingContext createContext(Map<String, Object> jsonMap) {
    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
    final JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext,
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
