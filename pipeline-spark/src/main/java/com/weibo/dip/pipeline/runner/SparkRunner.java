package com.weibo.dip.pipeline.runner;

import com.weibo.dip.pipeline.extract.FileTableExtractor;
import com.weibo.dip.pipeline.sink.DatasetDataSink;
import com.weibo.dip.pipeline.sink.DatasetSinkTypeEnum;
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

  private DatasetDataSink datasetSink;

  @SuppressWarnings({"unchecked"})
  public SparkRunner(Map<String, Object> configs) {
    super(configs);
    datasetSink = DatasetSinkTypeEnum.getDatasetSinkByMap(sinkConfig);
  }

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
    return sparkSession
        .read()
        .format(sourceFormat)
        .options(sourceOptions).load();
  }

  private void write(Dataset dataset) {
    datasetSink.write(dataset);
  }

  @Override
  public void stop() throws Exception {
    sparkSession.stop();
  }
}
