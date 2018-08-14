package com.weibo.dip.pipeline.source;

import com.weibo.dip.pipeline.util.HadoopFileUtil;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.HdfsUnionRDD;
import org.apache.spark.streaming.HdfsDStream;
import org.apache.spark.streaming.HdfsTimeDStream;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


/**
 * 按batch time读取对应时间段的文件，生成DStream
 * Create by hongxun on 2018/7/5
 */
public class StreamingHdfsTimeDataSource extends StreamingDataSource {

  private String path;
  private String category;
  private String blackList;
  private String backupDir;
  private String checkpointDirectory;
  private Integer multiple = 3;

  // todo: HdfsTime Dstream
  public StreamingHdfsTimeDataSource(Map map) {
    super(map);
    Map<String, Object> parameters = (Map<String, Object>) map.get("options`  `");
    path = (String) parameters.get("hdfsPath");
    category = (String) parameters.get("category");
    blackList = (String) parameters.get("blackList");
    backupDir = (String) parameters.get("backupDir");
    checkpointDirectory = (String) parameters.get("checkpointDirectory");
    Number f = (Number) parameters.get("multiple");
    if (f != null) {
      multiple = f.intValue();
    }
  }

  @Override
  public void stop() {

  }

  @Override
  public JavaDStream createSource(JavaStreamingContext streamingContext) {
    try {
      List<String> list = HadoopFileUtil.listCategory(path, category);
      System.out.println(list);
      if (blackList != null) {
        for (Iterator<String> iterator = list.iterator(); iterator.hasNext(); ) {
          if (blackList.contains(iterator.next())) {
            iterator.remove();
          }
        }
      }
      JavaInputDStream<Tuple2<Text, Text>> hdfsStream;
      //如果checkpoint目录不存在或者没有数据，backupDir不为空，读取backupdir的值进行恢复；
      if (checkpointDirectory != null && (!HadoopFileUtil.exists(checkpointDirectory)
          || HadoopFileUtil.listCategory(checkpointDirectory, "checkpoint-.*").isEmpty())
          && backupDir != null) {
        HadoopFileUtil.mkDir(backupDir);
        List<String> paths = HadoopFileUtil.listPaths(backupDir);
        if (!paths.isEmpty()) {
          String largest = paths.get(paths.size() - 1);
          Long lastBatchTime = Long.parseLong(largest.substring(largest.lastIndexOf("/") + 1));
          hdfsStream = HdfsTimeDStream
              .apply(streamingContext.ssc(), path, list.toArray(new String[0]), lastBatchTime,
                  multiple);
        } else {
          hdfsStream = HdfsDStream.apply(streamingContext.ssc(), path, list.toArray(new String[0]));
        }

      } else {
        hdfsStream = HdfsDStream.apply(streamingContext.ssc(), path, list.toArray(new String[0]));
      }
      if (backupDir != null) {
        hdfsStream.foreachRDD(rdd -> {
          try {
            long batchTime = ((HdfsUnionRDD) rdd.rdd()).batchTime();
            List<String> paths = HadoopFileUtil.listPaths(backupDir);
            for (int i = 0; i < paths.size() - 5; i++) {
              HadoopFileUtil.deleteFile(paths.get(i));
            }
            HadoopFileUtil
                .createAndWriteString(backupDir + "/" + batchTime, batchTime + "", "UTF-8", false);
          } catch (Exception e) {
            e.printStackTrace();
          }

        });
      }
      return hdfsStream.map(new Function<Tuple2<Text, Text>, String>() {
        public String call(Tuple2<Text, Text> v1) throws Exception {
          return v1._1.toString() + "`" + v1._2.toString() + "`";
        }
      });

    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}

