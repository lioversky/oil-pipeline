package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Assert;
import org.junit.Test;

public class ReplaceProcessorTest {

  private String test_type = "processor_replace";

  private String test_time = "2018-06-27 21:36:00";
  private long test_timestamp = 1530106560000l;
  private String formatStr = "yyyy-MM-dd HH:mm:ss";
  private DateTimeFormatter format = DateTimeFormat.forPattern(formatStr);

  @Test
  public void testStrToDate() {
    String fieldName = "str_to_date";

    try {
      Map<String, Object> params = ImmutableMap
          .of("fieldNotExistError", false, "subType", "replace_str_date", "fieldName", fieldName,
              "params", ImmutableMap.of(
                  "source", formatStr));
      Processor p = ProcessorTypeEnum.getType(test_type)
          .getProcessor(params);

      Map<String, Object> result = p
          .process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertTrue(result.get(fieldName) instanceof Date);
      Assert.assertEquals(test_time, new DateTime(result.get(fieldName)).toString(format));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(0);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertTrue(result.get(fieldName) instanceof Date);
      Assert.assertEquals(test_time, new DateTime(result.get(fieldName)).toString(format));


    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testStrToTimestamp() {
    String fieldName = "replace_str_timestamp";

    try {
      Map<String, Object> params = ImmutableMap
          .of("fieldNotExistError", true, "subType", "replace_str_timestamp", "fieldName",
              fieldName,
              "params", ImmutableMap.of(
                  "source", formatStr));
      Processor p = ProcessorTypeEnum.getType(test_type)
          .getProcessor(params);
      Map<String, Object> result = p
          .process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertTrue(result.get(fieldName) instanceof Long);
      Assert.assertEquals(test_timestamp, result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(1);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertTrue(result.get(fieldName) instanceof Long);
      Assert.assertEquals(test_timestamp, result.get(fieldName));

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testStrToUnixTimestamp() {
    String fieldName = "replace_str_unix";

    try {
      Map<String, Object> params = ImmutableMap
          .of("fieldNotExistError", true, "subType", "replace_str_unix", "fieldName", fieldName,
              "params", ImmutableMap.of(
                  "source", formatStr));
      Processor p = ProcessorTypeEnum.getType(test_type)
          .getProcessor(params);
      Map<String, Object> result = p
          .process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertTrue(result.get(fieldName) instanceof Long);
      Assert.assertEquals(test_timestamp / 1000, result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(2);

      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertTrue(result.get(fieldName) instanceof Long);
      Assert.assertEquals(test_timestamp / 1000, result.get(fieldName));

    } catch (Exception e) {
      Assert.fail();
    }
  }


  @Test
  public void testStrToDateStr() {
    String fieldName = "replace_str_str";

    try {
      Map<String, Object> params = ImmutableMap
          .of("fieldNotExistError", true, "subType", "replace_str_str", "fieldName", fieldName,
              "params", ImmutableMap.of(
                  "source", formatStr, "target", "yyyyMMdd HH:mm:ss"));
      Processor p = ProcessorTypeEnum.getType(test_type)
          .getProcessor(params);
      Map<String, Object> result = p
          .process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertEquals("20180627 21:36:00", result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(3);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertEquals("20180627 21:36:00", result.get(fieldName));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testUnixToDateStr() {
    String fieldName = "replace_unix_str";

    try {
      Map<String, Object> params = ImmutableMap
          .of("fieldNotExistError", true, "subType", "replace_unix_str", "fieldName", fieldName,
              "params",
              ImmutableMap.of(
                  "source", formatStr));
      Processor p = ProcessorTypeEnum.getType(test_type)
          .getProcessor(params);
      Map<String, Object> result = p.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_timestamp / 1000 + "")));
      Assert.assertEquals(test_time, result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(4);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_timestamp / 1000 + "")));
      Assert.assertEquals(test_time, result.get(fieldName));

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testTimestampToDateStr() {
    String fieldName = "replace_timestamp_str";

    try {
      Map<String, Object> params = ImmutableMap
          .of("fieldNotExistError", true, "subType", "replace_timestamp_str", "fieldName",
              fieldName, "params",
              ImmutableMap.of(
                  "source", formatStr));
      Processor p = ProcessorTypeEnum.getType(test_type)
          .getProcessor(params);
      Map<String, Object> result = p.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_timestamp + "")));
      Assert.assertEquals(test_time, result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(5);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_timestamp + "")));
      System.out.println(result);
      Assert.assertEquals(test_time, result.get(fieldName));
    } catch (Exception e) {
      Assert.fail();
    }
  }


  @Test
  public void testRegex() {
    String fieldName = "replace_regex";

    Map<String, Object> params = ImmutableMap
        .of("fieldNotExistError", true, "subType", "replace_regex", "fieldName", fieldName,
            "params",
            ImmutableMap.of("regex", "[a-z]+", "target", ""));
    Processor p = ProcessorTypeEnum.getType(test_type)
        .getProcessor(params);
    try {
      Map<String, Object> result = p.process(Maps.newHashMap(ImmutableMap.of(fieldName, "aaabbbcccddd123456")));
      System.out.println(result);
      Assert.assertEquals("123456", result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(6);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, "aaabbbcccddd123456")));
      Assert.assertEquals("123456", result.get(fieldName));

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testReplaceStr() {
    String fieldName = "replace_replace_str";

    Map<String, Object> params = ImmutableMap
        .of("fieldNotExistError", true, "subType", "replace_replace_str", "fieldName", fieldName,
            "params",
            ImmutableMap.of("source", "123456", "target", ""));
    Processor p = ProcessorTypeEnum.getType(test_type)
        .getProcessor(params);
    try {
      Map<String, Object> result = p.process(Maps.newHashMap(ImmutableMap.of(fieldName, "aaabbbcccddd123456")));
      Assert.assertEquals("aaabbbcccddd", result.get(fieldName));

      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);
      Processor p1 = processorList.get(7);
      result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, "aaabbbcccddd123456")));
      Assert.assertEquals("aaabbbcccddd", result.get(fieldName));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private String jsonFile = "src/test/resources/sample_pipeline_replace.json";


}
