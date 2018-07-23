package com.weibo.dip.pipeline.util;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.functions;

/**
 * Create by hongxun on 2018/7/23
 */
public class DatasetUtil {

  public static Dataset splitDataset(Dataset dataset, String fieldName,String arrName, String splitStr,
      String[] targetFields) {
    dataset = dataset.withColumn(arrName, functions.split(col(fieldName), splitStr));
    for (int i = 0; i < targetFields.length; i++) {
      dataset = dataset.withColumn(targetFields[i], col(arrName).getItem(i));
    }
    return dataset.drop(arrName);
  }

}
