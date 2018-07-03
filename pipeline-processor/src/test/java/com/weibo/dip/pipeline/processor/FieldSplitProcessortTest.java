package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Create by hongxun on 2018/7/1
 */
public class FieldSplitProcessortTest {

  private String jsonFile = "src/test/resources/sample_pipeline_fieldsplit.json";
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
  public void testStr() {
    String fieldName = "split";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName, "a,b,c"));

    try {
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
  public void testRegex() {
    String fieldName = "split";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName, ""));
    try {

      Processor p1 = processorList.get(3);
      Map<String, Object> result = p1
          .process(data);
      Assert.assertTrue(result.containsKey("a") && "a".equals(result.get("a")));
      Assert.assertEquals(4, result.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
//    todo:
  }

  @Test
  public void testJson() {
    String fieldName = "split";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName,
            "{\"fieldNotExistError\": false,\"fieldName\": \"split\"}"));
    try {

      Processor p1 = processorList.get(4);
      Map<String, Object> result = p1
          .process(data);

      Assert.assertTrue(result.containsKey("map"));
      Object value = result.get("map");
      Assert.assertTrue(value instanceof Map);

      Map<String, Object> map = (Map<String, Object>) value;
      Assert.assertEquals(2, map.size());
      Assert.assertEquals("split", map.get("fieldName"));
      Assert.assertFalse((boolean) map.get("fieldNotExistError"));


    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
//    todo:
  }

  @Test
  public void testList() {
    String fieldName = "split";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName, Lists.newArrayList("a", "b", "c")));

    try {

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
  @Test
  public void testMulti() {
    String fieldName = "split";
    HashMap<String, Object> data = Maps
        .newHashMap(ImmutableMap.of(fieldName, "a,b,c,d"));

    try {
      Processor p1 = processorList.get(5);
      Map<String, Object> result = p1
          .process(data);
      Assert.assertTrue(result.containsKey("d") && "d".equals(result.get("d")));
      Assert.assertEquals(5, result.size());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

}
