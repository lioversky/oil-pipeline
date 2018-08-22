package com.weibo.dip.pipeline;

import com.weibo.dip.pipeline.runner.Runner;
import com.weibo.dip.util.GsonUtil;
import java.lang.reflect.Constructor;
import java.util.Map;

/**
 * Create by hongxun on 2018/8/2
 */
public class PipelineMain {

  public static void main(String[] args) {
    String jsonFile = args[0];
    try {
      Map<String, Object> config = GsonUtil
          .loadJsonFromFile(jsonFile, GsonUtil.GsonType.OBJECT_MAP_TYPE);
      Map<String, Object> applicationConfig = (Map<String, Object>) config.get("applicationConfig");
      String engineType = (String) applicationConfig.remove("engineType");
      Constructor<Runner> constructor = (Constructor<Runner>) Class.forName(engineType)
          .getConstructor(Map.class);
      Runner runner = constructor.newInstance((Map<String, Object>) config);
      runner.start();
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        try {
          runner.stop();
        } catch (Exception e) {

        }
      }));
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
