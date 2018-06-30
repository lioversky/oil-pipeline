package com.weibo.dip.pipeline;

import com.weibo.dip.util.GsonUtil;
import com.weibo.dip.util.GsonUtil.GsonType;
import java.util.Map;
import org.junit.Test;

public class StageTest {

  @Test
  public void testCreateStage() {
    try {
      Map<String, Object> stages =
          GsonUtil.loadJsonFromFile("", GsonType.OBJECT_MAP_TYPE);


    } catch (Exception e) {
      e.printStackTrace();
    }
  }


}
