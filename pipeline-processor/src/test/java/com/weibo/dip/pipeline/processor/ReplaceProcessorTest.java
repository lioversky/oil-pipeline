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
import org.junit.Before;
import org.junit.Test;

public class ReplaceProcessorTest {

  private String test_type = "processor_replace";

  private String test_time = "2018-06-27 21:36:00";
  private long test_timestamp = 1530106560000l;
  private String formatStr = "yyyy-MM-dd HH:mm:ss";
  private DateTimeFormatter format = DateTimeFormat.forPattern(formatStr);
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
  public void testStrToDate() {
    String fieldName = "str_to_date";

    try {

      Processor p1 = processorList.get(0);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
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
      Processor p1 = processorList.get(1);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
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

      Processor p1 = processorList.get(2);

      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
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

      Processor p1 = processorList.get(3);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_time)));
      Assert.assertEquals("20180627 21:36:00", result.get(fieldName));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testUnixToDateStr() {
    String fieldName = "replace_unix_str";

    try {

      Processor p1 = processorList.get(4);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_timestamp / 1000 + "")));
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

      Processor p1 = processorList.get(5);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, test_timestamp + "")));
      System.out.println(result);
      Assert.assertEquals(test_time, result.get(fieldName));
    } catch (Exception e) {
      Assert.fail();
    }
  }


  @Test
  public void testRegex() {
    String fieldName = "replace_regex";
    try {

      Processor p1 = processorList.get(6);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, "aaabbbcccddd123456")));
      Assert.assertEquals("123456", result.get(fieldName));

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testReplaceStr() {
    String fieldName = "replace_replace_str";
    try {

      Processor p1 = processorList.get(7);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, "aaabbbcccddd123456")));
      Assert.assertEquals("aaabbbcccddd", result.get(fieldName));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private String jsonFile = "src/test/resources/sample_pipeline_replace.json";


}
