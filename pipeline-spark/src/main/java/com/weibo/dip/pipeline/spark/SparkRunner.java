package com.weibo.dip.pipeline.spark;

import com.weibo.dip.pipeline.udf.UDFRegister;
import com.weibo.dip.pipeline.util.DatasetUtil;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 离线spark的runner
 * Create by hongxun on 2018/7/5
 */

public class SparkRunner extends DatasetRunner {


  @SuppressWarnings({"unchecked"})
  public SparkRunner(Map<String, Object> configs) {
    super(configs);
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
      DatasetUtil.cache(sparkSession, tables);
    }
    //抽取
    Dataset extractDataset = extract(sourceDataset);
    //处理
    Dataset resultDataset = process(extractDataset);
    write(resultDataset);
  }


  private Dataset<Row> loadSparkDataSet() {
    return sparkSession
        .read()
        .format(sourceFormat)
        .options(sourceOptions).load();
  }

  private void write(Dataset dataset) {
    dataset.write()
        .mode(sinkMode)
        .format(sinkFormat)
        .options(sinkOptions)
        .save();
  }

  @Override
  public void stop() throws Exception {
    sparkSession.stop();
  }
}
