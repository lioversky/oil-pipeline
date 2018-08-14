package com.weibo.dip.pipeline.runner;

import com.weibo.dip.pipeline.udf.UDFRegister;
import com.weibo.dip.pipeline.util.SparkUtil;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * StructuredStreaming的Runner
 * Create by hongxun on 2018/7/5
 */
public class StructuredStreamingRunner extends DatasetRunner {


  private StreamingQuery query;

  public StructuredStreamingRunner(Map<String, Object> configs) {
    super(configs);
  }

  /**
   * runner的start方法实现.
   * 创建SparkSession实例；注册udf；cache table；创建流式Dataset；process；output；
   */
  @Override
  public void start() throws Exception {
    //创建SparkSession
    sparkSession = SparkSession.builder().master("local").getOrCreate();
    //注册udf
    UDFRegister.registerAllUDF(sparkSession);
    //加载source源
    Dataset<Row> sourceDataset = loadStreamDataSet();
    //其它依赖数据源
    if (tables != null) {
      SparkUtil.cache(sparkSession, tables);
    }
    //抽取
    Dataset extractDataset = extract(sourceDataset);
    //处理
    Dataset resultDataset = process(extractDataset);
    //写出
    query = writeStream(resultDataset);
    query.awaitTermination();

  }

  /**
   * structured streaming source
   *
   * @return dataset
   */
  private Dataset<Row> loadStreamDataSet() {
    return sparkSession
        .readStream()
        .format(sourceFormat)
        .options(sourceOptions)
        .load();
  }

  /**
   * 抽取数据
   *
   * @param dataset 数据集
   * @return 抽取结果数据集
   */
  protected Dataset extract(Dataset dataset) {
    if (extractor == null) {
      return dataset;
    }
    if ("kafka".equals(sourceFormat)) {
      dataset = dataset.selectExpr("CAST(value AS STRING) as _value_");
    }
    return extractor.extract(dataset);
  }

  /**
   * structured streaming sink
   *
   * @param dataset 数据
   * @return query
   */
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
    sparkSession.stop();
  }
}
