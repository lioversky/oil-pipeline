package com.weibo.dip.pipeline.udf;

import java.util.ServiceLoader;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataType;

/**
 * 通过service配置文件动态加载.
 * Create by hongxun on 2018/7/10
 */
public abstract class UDFRegister {

  protected String udfName;
  private static ServiceLoader<UDFRegister> registerServiceLoader = ServiceLoader
      .load(UDFRegister.class);

  public abstract void register(SparkSession sparkSession);

  public static void registerAllUDF(SparkSession sparkSession) {
    for (UDFRegister register : registerServiceLoader) {
      register.register(sparkSession);
    }
  }

  public static void registerUDF(SparkSession sparkSession, UDFRegister register) {
    register.register(sparkSession);
  }

  public static void registerUDF(SparkSession sparkSession, String name, UDF1 udf1,
      DataType dataType) {
    sparkSession.udf().register(name, udf1, dataType);
  }

  public static void registerUDF(SparkSession sparkSession, String name, UDF2 udf2,
      DataType dataType) {
    sparkSession.udf().register(name, udf2, dataType);
  }

  public static void registerUDF(SparkSession sparkSession, String name, UDF3 udf3,
      DataType dataType) {
    sparkSession.udf().register(name, udf3, dataType);
  }
}
