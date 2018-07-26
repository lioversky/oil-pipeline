package com.weibo.dip.pipeline.runner;

import com.weibo.dip.pipeline.udf.UDFRegister;
import com.weibo.dip.pipeline.util.DatasetUtil;
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
    //其它依赖数据源
    if (tables != null) {
      DatasetUtil.cache(sparkSession, tables);
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
