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
      Assert.fail("create processorList error!!!");
    }
  }

  /**
   * 测试复制
   */
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


  /**
   * 测试复制，目标列存在抛异常
   */
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


  /**
   * 增加当前时间字符串
   */
  @Test
  public void testAddCurDateStr() {
    String targetField = "datestr";
    String dateFormat = "yyyyMMdd HH:mm:ss";
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("copy", "copydata"));
    Map<String, Object> params = ImmutableMap
        .of("subType", "fieldadd_datestr", "params",
            ImmutableMap.of("targetField", targetField, "dateFormat", dateFormat));
    Processor<Map<String, Object>> p = Processor.createProcessor(test_type, params);

    try {
      Map<String, Object> result = p.process(data);
      Assert.assertTrue(result.containsKey(targetField));
      Assert.assertEquals(dateFormat.length(), result.get(targetField).toString().length());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 增加当前时间unix时间戳
   */
  public void testAddCurUnixStr() {
    String targetField = "datestr";
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("copy", "copydata"));
    Map<String, Object> params = ImmutableMap
        .of("targetField", targetField, "subType", "fieldadd_unixtimestamp");
    Processor<Map<String, Object>> p = Processor.createProcessor(test_type, params);

    try {
      Map<String, Object> result = p.process(data);
      System.out.println(result);
      Assert.assertTrue(result.containsKey(targetField));
      Assert.assertTrue(result.get(targetField) instanceof Number);
      Assert.assertEquals(10, result.get(targetField).toString().length());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * 增加当前时间戳
   */
  public void testAddCurTimestampStr() {
    String targetField = "datestr";
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("copy", "copydata"));
    Map<String, Object> params = ImmutableMap
        .of("targetField", targetField, "subType", "fieldadd_timestamp");
    Processor<Map<String, Object>> p = Processor.createProcessor(test_type, params);

    try {
      Map<String, Object> result = p.process(data);
      System.out.println(result);
      Assert.assertTrue(result.containsKey(targetField));
      Assert.assertTrue(result.get(targetField) instanceof Number);
      Assert.assertEquals(13, result.get(targetField).toString().length());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

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
