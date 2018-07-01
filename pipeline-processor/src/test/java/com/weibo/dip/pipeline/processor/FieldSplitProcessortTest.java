package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Create by hongxun on 2018/7/1
 */
public class FieldSplitProcessortTest {

  private String jsonFile = "src/test/resources/sample_pipeline_fieldsplit.json";

  @Test
  public void testStr() {
    String fieldName = "split";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName, "a,b,c"));

    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(0);
      Map<String, Object> result = p1
          .process(data);
      Assert.assertTrue(result.containsKey("a") && "a".equals(result.get("a")));
      Assert.assertEquals(4, result.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testList() {
    String fieldName = "split";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName, Lists.newArrayList("a", "b", "c")));

    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(1);
      Map<String, Object> result = p1
          .process(data);
      Assert.assertTrue(result.containsKey("a") && "a".equals(result.get("a")));
      Assert.assertEquals(4, result.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testArray() {
    String fieldName = "split";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName, new Object[]{"a", "b", "c"}));

    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(2);
      Map<String, Object> result = p1
          .process(data);
      Assert.assertTrue(result.containsKey("a") && "a".equals(result.get("a")));
      Assert.assertEquals(4, result.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

}
