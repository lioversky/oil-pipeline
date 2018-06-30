package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class FieldMergeProcessorTest {

  private String test_type = "processor_fieldmerge";

  /*@Test
  public void testMerge() {
    String fields = "a,b,c";
    String fieldName = "c";
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aaa", "b", "bbb", "c", "ccc"));

    Map<String, Object> params = ImmutableMap
        .of("overwriteIfFieldExist", true, "fields", fields, "splitStr", "", "toFieldName",
            fieldName);
    Processor p = ProcessorTypeEnum.getType(test_type)
        .getProcessor(params);
    try {
      Map<String, Object> result = p.process(data);
      System.out.println(result);
      Assert.assertEquals("aaabbbccc", result.get(fieldName));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test(expected = FieldExistException.class)
  public void testMergeOvewriteError() throws Exception {
    String fields = "a,b,c";
    String fieldName = "c";
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aaa", "b", "bbb", "c", "ccc"));

    Map<String, Object> params = ImmutableMap
        .of("overwriteIfFieldExist", false, "fields", fields, "splitStr", "", "toFieldName",
            fieldName);
    Processor p = ProcessorTypeEnum.getType(test_type)
        .getProcessor(params);
    Map<String, Object> result = p.process(data);
    Assert.fail();
  }*/

  private String jsonFile = "src/test/resources/sample_pipeline_fieldmerge.json";

  @Test
  public void testJsonMergeStr() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aa", "b", "bb", "c", "cc"));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      Processor p = processorList.get(0);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("merge"));
      Assert.assertEquals("aa,bb,cc", data.get("merge"));

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testJsonMergeList() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aa", "b", "bb", "c", "cc"));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      Processor p = processorList.get(1);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("merge") && data.get("merge") instanceof List);
      Assert.assertEquals("aa", ((List) data.get("merge")).get(0));
      Assert.assertEquals(3, ((List) data.get("merge")).size());

      data = Maps.newHashMap(ImmutableMap.of("a", "aa", "c", "cc"));
      data = p.process(data);
      Assert.assertEquals(2, ((List) data.get("merge")).size());

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testJsonMergeListKeepNull() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aa", "c", "cc"));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      Processor p = processorList.get(2);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("merge") && data.get("merge") instanceof List);
      Assert.assertEquals("aa", ((List) data.get("merge")).get(0));
      Assert.assertEquals(3, ((List) data.get("merge")).size());
      Assert.assertTrue(((List) data.get("merge")).get(1) == null);

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testJsonMergeSet() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aa", "b", "bb", "c", "cc"));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      Processor p = processorList.get(3);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("merge") && data.get("merge") instanceof Set);
      Assert.assertEquals(3, ((Set) data.get("merge")).size());

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testJsonMergeMap() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("a", "aa", "b", "bb", "c", "cc"));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      Processor p = processorList.get(4);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("merge") && data.get("merge") instanceof Map);
      Assert.assertEquals(3, ((Map<String, Object>) data.get("merge")).size());
      Assert.assertEquals("aa", ((Map<String, Object>) data.get("merge")).get("a"));
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
