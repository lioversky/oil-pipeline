package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.exception.FieldNotExistException;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class SubStringProcessorTest {

  private String test_type = "processor_substring";
  private String jsonFile = "src/test/resources/sample_pipeline_substring.json";

  @Test
  public void testTrimFieldExist() {
    String fieldName = "substring_trim";

    Map<String, Object> params = ImmutableMap
        .of("fieldNotExistError", false, "subType", "substring_trim", "fieldName", fieldName);
    Processor p = ProcessorTypeEnum.getType(test_type)
        .getProcessor(params);

    try {
      Map<String, Object> result = p
          .process(Maps.newHashMap(ImmutableMap.of(fieldName, " trimdata ")));
      Assert.assertEquals("trimdata", result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(0);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, " trimdata ")));
      Assert.assertEquals("trimdata", result.get(fieldName));

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test(expected = FieldNotExistException.class)
  public void testTrimFieldNotExist() throws Exception {
    String fieldName = "trim";
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("copy1", "copydata"));

    Map<String, Object> params = ImmutableMap
        .of("fieldNotExistError", true, "subType", "substring_trim", "fieldName", fieldName);
    Processor p = ProcessorTypeEnum.getType(test_type)
        .getProcessor(params);

    Map<String, Object> result = p.process(data);

    Assert.fail();
  }


  @Test
  public void testFixedSubStringer() {
    String fieldName = "substring_fixed";

    Map<String, Object> params = ImmutableMap
        .of("fieldNotExistError", false, "subType", "substring_fixed", "fieldName", fieldName,
            "params",
            ImmutableMap.of(
                "begin", 4, "end", 4));
    Processor p = ProcessorTypeEnum.getType(test_type)
        .getProcessor(params);

    try {
      Map<String, Object> result = p
          .process(Maps.newHashMap(ImmutableMap.of(fieldName, "111_fixed_111")));
      System.out.println(result);
      Assert.assertEquals("fixed", result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(1);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, "111_fixed_111")));
      System.out.println(result);
      Assert.assertEquals("fixed", result.get(fieldName));


    } catch (Exception e) {
      e.printStackTrace();
    }
  }


  @Test
  public void testMatchSubStringer() {
    String fieldName = "substring_match";

    Map<String, Object> params = ImmutableMap
        .of("fieldNotExistError", false, "subType", "substring_match", "fieldName", fieldName,
            "params",
            ImmutableMap.of("beginStr", "111_", "endStr", "_111"));
    Processor p = ProcessorTypeEnum.getType(test_type)
        .getProcessor(params);

    try {
      Map<String, Object> result = p
          .process(Maps.newHashMap(ImmutableMap.of(fieldName, "111_match_111")));
      System.out.println(result);
      Assert.assertEquals("match", result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(2);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, "111_match_111")));
      Assert.assertEquals("match", result.get(fieldName));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }


}
