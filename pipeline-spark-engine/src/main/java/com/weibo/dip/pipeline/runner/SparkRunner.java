package com.weibo.dip.pipeline.runner;

import com.weibo.dip.pipeline.register.FileTableExtractor;
import com.weibo.dip.pipeline.sink.DatasetDataSink;
import com.weibo.dip.pipeline.sink.Sink;
import com.weibo.dip.pipeline.source.DatasetSource;
import com.weibo.dip.pipeline.source.Source;
import com.weibo.dip.pipeline.udf.UDFRegister;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 离线spark的runner
 * Create by hongxun on 2018/7/5
 */

public class SparkRunner extends DatasetRunner {

  private DatasetSource datasetSource;
  private DatasetDataSink datasetSink;

  /**
   * 构造函数
   * @param configs 参数
   */
  @SuppressWarnings({"unchecked"})
  public SparkRunner(Map<String, Object> configs) {
    super(configs);
    datasetSource = (DatasetSource) Source.createSource(sourceFormat, sourceConfig);
    datasetSink = (DatasetDataSink) Sink.createSink(sinkFormat,sinkConfig);
  }

  /**
   * runner的start方法实现.
   * 创建SparkSession实例；注册udf；创建源dataset；cache table；process；output
   */
  @Override
  public void start() throws Exception {
    //创建SparkSession
    sparkSession = SparkSession.builder().master("local").getOrCreate();
    //注册udf
    UDFRegister.registerAllUDF(sparkSession);
    //加载source源
    Dataset<Row> sourceDataset = loadSparkDataSet();
    //其它依赖数据源
    if (tables != null) {
      FileTableExtractor.cacheTable(sparkSession, tables);
    }
    //抽取
    Dataset extractDataset = extract(sourceDataset);
    //处理
    Dataset resultDataset = process(extractDataset);
    write(resultDataset);
  }


  private Dataset<Row> loadSparkDataSet() {
    if (sourceFormat == null) {
      return sparkSession.emptyDataFrame();
    }
    // todo:spark dataset source

    return datasetSource.createSource(sparkSession);
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
    return extractor.extract(dataset);
  }

  private void write(Dataset dataset) {
    datasetSink.write(dataset);
  }

  @Override
  public void stop() throws Exception {
    sparkSession.stop();
  }
}
