package com.weibo.dip.pipeline.udf;

import com.weibo.dip.util.StringUtil;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * url参数提取udf
 * udf参数：字段值
 * Create by hongxun on 2018/7/10
 */
public class UrlArgsSplitUDFRegister extends UDFRegister {

  public UrlArgsSplitUDFRegister() {
    udfName = "urlargs_split";
  }

  @Override
  public void register(SparkSession sparkSession) {
    sparkSession.udf().register(udfName, (String value) -> StringUtil.urlArgsSplit(value),
        DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType));
  }
}
