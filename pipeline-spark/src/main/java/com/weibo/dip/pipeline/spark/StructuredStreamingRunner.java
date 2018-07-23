package com.weibo.dip.pipeline.spark;

import com.codahale.metrics.MetricRegistry;
import com.weibo.dip.pipeline.extract.DatasetExactorTypeEnum;
import com.weibo.dip.pipeline.extract.DatasetExtractor;
import com.weibo.dip.pipeline.runner.Runner;
import com.weibo.dip.pipeline.stage.DatasetProcessStage;
import com.weibo.dip.pipeline.stage.Stage;
import com.weibo.dip.pipeline.udf.UDFRegister;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * Create by hongxun on 2018/7/5
 */
public class StructuredStreamingRunner extends Runner {

  private SparkSession sparkSession;
  //  spark执行类型
  private String engineType;
  private String sourceFormat;
  private Map<String, String> sourceOptions;

  private Map<String, Object> preConfig;
  private Map<String, Object> aggConfig;
  private Map<String, Object> proConfig;

  private String sinkFormat;
  private String sinkMode;
  private Map<String, String> sinkOptions;

  //  抽取器
  private DatasetExtractor extractor;

  private StreamingQuery query;

  public StructuredStreamingRunner(Map<String, Object> configs) {
    //source配置
    engineType = (String) configs.get("engineType");
    Map<String, Object> sourceConfig = (Map<String, Object>) configs.get("sourceConfig");
    sourceFormat = (String) sourceConfig.get("format");
    sourceOptions = (Map<String, String>) sourceConfig.get("options");
    Map<String, Object> extractConfig = (Map<String, Object>) sourceConfig.get("extractor");
    extractor = DatasetExactorTypeEnum.getType(extractConfig);
    //process配置
    Map<String, Object> processConfig = (Map<String, Object>) configs.get("processConfig");
    preConfig = (Map<String, Object>) processConfig.get("pre");
    aggConfig = (Map<String, Object>) processConfig.get("agg");
    proConfig = (Map<String, Object>) processConfig.get("pro");
    //sink配置
    Map<String, Object> sinkConfig = (Map<String, Object>) configs.get("sinkConfig");
    sinkFormat = (String) sinkConfig.get("format");
    sinkMode = (String) sinkConfig.get("mode");
    sinkOptions = (Map<String, String>) sinkConfig.get("options");

  }

  /**
   * 启动runner
   * 先生成stream source
   */
  @Override
  public void start() throws Exception {
    //创建SparkSession
    sparkSession = SparkSession.builder().master("local").getOrCreate();
    //注册udf
    UDFRegister.registerAllUDF(sparkSession);
    //加载source源
    Dataset<Row> sourceDataset = loadStreamDataSet();
    //抽取
    Dataset extractDataset = extract(sourceDataset);
    //处理
    Dataset resultDataset = process(extractDataset);
    //写出
    query = writeStream(resultDataset);
    query.awaitTermination();

  }

  private Dataset extract(Dataset dataset) {
    if ("kafka".equals(sourceFormat)) {
      dataset = dataset.selectExpr("CAST(value AS STRING) as _value_");
    }
    return extractor.extract(dataset);
  }

  /**
   *
   * @param dataset
   * @return
   * @throws Exception
   */
  private Dataset process(Dataset dataset) throws Exception {
    if (preConfig != null) {
      dataset = pre(dataset);
    }
    if (aggConfig != null) {
      dataset = agg(dataset);
    }
    if (proConfig != null) {
      dataset = pro(dataset);
    }
    return dataset;
  }

  /**
   * 前处理阶段，在StructuredRunner只取第一个stage，因为没有条件所以全部可以在一个stage里
   *
   * @param dataset 数据集
   * @return 处理后数据集
   */
  private Dataset pre(Dataset dataset) throws Exception {
    List<Map<String, Object>> stagesConfigList = (List<Map<String, Object>>) preConfig
        .get("stages");
    if (!stagesConfigList.isEmpty()) {
      Map<String, Object> stageConfigMap = stagesConfigList.get(0);
      List<Map<String, Object>> processorConfigList = (List<Map<String, Object>>) stageConfigMap
          .get("processors");
      DatasetProcessStage processStage = new DatasetProcessStage(new MetricRegistry(),
          processorConfigList, Stage.createStageId("DatasetProcess"));
      return processStage.processStage(dataset);
    }
    return dataset;
  }

  private Dataset agg(Dataset dataset) {
    return dataset;
  }

  private Dataset pro(Dataset dataset) {
    return dataset;
  }




/*

    Encoder<Map<String, Object>> mapEncoder = Encoders
        .kryo((Class<Map<String, Object>>) (Class) Map.class);
    Encoder<Row> rowEncoder = Encoders.kryo(Row.class);

    FlatMapFunction<String, Map<String, Object>> extractFunction = x -> extractor.extract(x)
        .iterator();

    MapPartitionsFunction<Map<String, Object>, Row> processFunction = iterator -> {
      List<Row> rows = Lists.newArrayList();
      while (iterator.hasNext()) {
        Map<String, Object> data = iterator.next();
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

    Dataset<String> ds = dataset.selectExpr("CAST(value AS STRING)").as(Encoders.STRING());
    Dataset<Map<String, Object>> extractDataSet = ds
        .flatMap(extractFunction, mapEncoder);

    List<StructField> inputFields = new ArrayList<>();
    for (String column : columns) {
      inputFields.add(DataTypes.createStructField(column, new ObjectType(Object.class), true));
    }
    StructType schema = DataTypes.createStructType(inputFields);

    Dataset<Row> resultDataSet = extractDataSet
        .mapPartitions(processFunction, rowEncoder).toDF(columns);

*/


  private Dataset<Row> loadStreamDataSet() {
    return sparkSession
        .readStream()
        .format(sourceFormat)
        .options(sourceOptions).load();
  }


  private StreamingQuery writeStream(Dataset dataset) {
    return dataset.writeStream()
        .outputMode(sinkMode)
        .format(sinkFormat)
        .options(sinkOptions)
        .start();
  }


  @Override
  public void stop() throws Exception {
    query.stop();
  }
}
