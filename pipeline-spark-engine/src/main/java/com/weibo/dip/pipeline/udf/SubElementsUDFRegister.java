package com.weibo.dip.pipeline.udf;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * 获取map字段里面部分属性udf
 * udf参数：字段map值 ,保留字段名(为空或者长度为0返回原值)
 * Create by hongxun on 2018/7/10
 */

public class SubElementsUDFRegister extends UDFRegister {

  public SubElementsUDFRegister() {
    udfName = "sub_element";
  }

  @Override
  public void register(SparkSession sparkSession) {
    sparkSession.udf()
        .register(udfName,
            (scala.collection.immutable.HashMap<String, String> dataMap,
                scala.collection.mutable.WrappedArray keepColNames) -> {
              if (keepColNames == null || keepColNames.size() == 0) {
                return dataMap;
              }
              Map<String, String> newData = new HashMap<>();
              Object[] array = (Object[]) keepColNames.array();
              for (Object col : array) {
                String colName = (String) col;
                if (dataMap.contains(colName)) {
                  newData.put(colName, dataMap.get(colName).get());
                }
              }
              return newData;
            },
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
  }
}
