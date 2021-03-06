package com.weibo.dip.pipeline.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * 左右定长截取子串udf
 * udf参数：字段值，左边截取长度，右边截取长度
 * Create by hongxun on 2018/7/10
 */

public class SubstringLRUDFRegister extends UDFRegister {

  public SubstringLRUDFRegister() {
    udfName = "substring_lr";
  }

  @Override
  public void register(SparkSession sparkSession) {
    sparkSession.udf()
        .register(udfName, (String value, Integer left, Integer right) ->
                StringUtils.substring(value, left, right < 0
                    ? value.length()
                    : value.length() - right),
            DataTypes.StringType);
  }
}
