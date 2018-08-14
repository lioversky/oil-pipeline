package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Create by hongxun on 2018/7/2
 */
public class FlattenProcessorTest {

  //  todo:
  private String jsonFile = "src/test/resources/sample_pipeline_flatten.json";

  private List<Processor> processorList;

  @Before
  public void before() {
    try {
      processorList = JsonTestUtil.getProcessors(jsonFile);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("create processorList error!!!");
    }
  }

  @Test
  public void testFlattenField() {
    String fieldName = "flatten";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName, ImmutableMap.of("a", 1, "b", 2)));

    try {
      Processor<Map<String, Object>> p1 = processorList.get(1);
      Map<String, Object> result = p1.process(data);
      Assert.assertTrue(result.containsKey("a"));
      Assert.assertEquals(3, result.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testFlattenAll() {
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of("f1", ImmutableMap.of("a", 1, "b", 2),
            "f2", ImmutableMap.of("c", 3, "b", 4)));

    try {
      Processor<Map<String, Object>> p1 = processorList.get(0);
      Map<String, Object> result = p1.process(data);
      Assert.assertTrue(result.containsKey("a"));
      Assert.assertEquals(5, result.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
