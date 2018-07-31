package com.weibo.dip.pipeline.udf;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * 按照正则切分成数组，如果不匹配返回空
 * udf参数：要切分的值，正则，字段名
 * Create by hongxun on 2018/7/25
 */
public class RegexToArrayUDFRegister extends UDFRegister {

  public RegexToArrayUDFRegister() {
    udfName = "regex_to_array";
  }

  @Override
  public void register(SparkSession sparkSession) {
    sparkSession.udf()
        .register(udfName,
            (String data, String regex, Integer fieldLength) -> {
              Pattern pattern = Pattern.compile(regex);
              Matcher matcher = pattern.matcher(data);
              String[] values = new String[fieldLength];
              if (matcher.find() && matcher.groupCount() == fieldLength) {
                for (int index = 1; index <= matcher.groupCount(); index++) {
                  values[index - 1] = matcher.group(index);
                }
                return values;
              } else {
                return null;
              }
            }, DataTypes.createArrayType(DataTypes.StringType));
  }
}
