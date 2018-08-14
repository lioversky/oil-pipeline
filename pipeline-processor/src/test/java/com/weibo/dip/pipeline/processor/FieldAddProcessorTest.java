package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.exception.FieldExistException;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FieldAddProcessorTest {

  private final static String test_type = "processor_fieldadd";
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
/*

  */
/**
   * 测试复制
   *//*

  @Test
  public void testCopyFieldNotExist() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("copy", "copydata"));
    Map<String, Object> params = ImmutableMap
        .of("params", ImmutableMap.of("sourceField", "copy", "targetField", "copy1"), "subType",
            "fieldadd_copy");
    Processor<Map<String, Object>> p = Processor.createProcessor(test_type, params);

    try {
      Map<String, Object> result = p.process(data);
      Assert.assertTrue(result.containsKey("copy1"));
      Assert.assertEquals("copydata", result.get("copy1"));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  */
/**
   * 测试复制，目标列存在抛异常
   *//*

  @Test(expected = FieldExistException.class)
  public void testCopyFieldExist() throws Exception {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("copy", "copydata", "copy1", ""));
    Map<String, Object> params = ImmutableMap
        .of("params", ImmutableMap.of("sourceField", "copy", "targetField", "copy1"), "subType",
            "fieldadd_copy",
            "overwriteIfFieldExist", false);
    Processor<Map<String, Object>> p = Processor.createProcessor(test_type, params);

    try {
      Map<String, Object> result = p.process(data);
      System.out.println(result);
    } catch (Exception e) {
      Assert.assertEquals(String.format("key %s exist!!!", "copy1"), e.getMessage());
      throw e;
    }
  }

*/

  private String jsonFile = "src/test/resources/sample_pipeline_fieldadd.json";

  @Test
  public void testJsonFiledCopy() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("field", "aaabbbccc"));
    try {

      Processor<Map<String, Object>> p = processorList.get(0);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("fieldcopy"));
      Assert.assertEquals("aaabbbccc", data.get("fieldcopy"));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testJsonAddDatestr() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("field", "aaabbbccc"));
    try {

      Processor<Map<String, Object>> p = processorList.get(1);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("datetime"));
      Assert.assertEquals(8, data.get("datetime").toString().length());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testJsonAddTimestamp() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("field", "aaabbbccc"));
    try {

      Processor<Map<String, Object>> p = processorList.get(2);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("timestamp"));
      Assert.assertEquals(13, data.get("timestamp").toString().length());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testJsonAddUnix() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("field", "aaabbbccc"));
    try {

      Processor<Map<String, Object>> p = processorList.get(3);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("unixtimestamp"));
      Assert.assertEquals(10, data.get("unixtimestamp").toString().length());

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

}
