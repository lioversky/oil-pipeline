package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FieldSelectProcessorTest {

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

  private String jsonFile = "src/test/resources/sample_pipeline_fieldremove.json";

  @Test
  public void testJsonRemove() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aa", "b", "bb", "c", "cc"));
    try {

      Processor<Map<String, Object>> p = processorList.get(0);
      data = p.process(data);
      System.out.println(data);
      Assert.assertFalse(data.containsKey("a"));
      Assert.assertEquals(1, data.size());

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testJsonKeep() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aa", "b", "bb", "c", "cc"));
    try {

      Processor<Map<String, Object>> p = processorList.get(1);
      data = p.process(data);
      Assert.assertFalse(data.containsKey("c"));
      Assert.assertEquals(2, data.size());

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testJsonRemoveNull() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("b", "bb", "c", "cc"));
    try {

      Processor<Map<String, Object>> p = processorList.get(2);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("b"));
      Assert.assertEquals(2, data.size());

    } catch (Exception e) {
      Assert.fail();
    }
  }
}
