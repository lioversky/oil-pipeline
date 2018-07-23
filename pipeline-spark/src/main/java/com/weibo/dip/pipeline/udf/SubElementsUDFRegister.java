package com.weibo.dip.pipeline.udf;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
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
            (scala.collection.immutable.HashMap<String, String> map, scala.collection.mutable.WrappedArray colNames) -> {
              Map<String, String> newData = new HashMap<>();
              Object[] array = (Object[])colNames.array();
              for (Object col : array) {
                String colName = (String) col;
                if (map.contains(colName)) {
                  newData.put(colName, map.get(colName).get());
                }
              }
              return newData;
            },
            DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
  }
}
