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
  public void testRegex() {
    String fieldName = "replace_regex";
    try {

      Processor<Map<String,Object>> p1 = processorList.get(0);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, "aaabbbcccddd123456")));
      Assert.assertEquals("123456", result.get(fieldName));

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testReplaceStr() {
    String fieldName = "replace_filekv";
    try {

      Processor<Map<String,Object>> p1 = processorList.get(1);
      Map<String, Object> result = p1.process(Maps.newHashMap(ImmutableMap.of(fieldName, "1111")));
      Assert.assertEquals("aaabbbcccddd", result.get(fieldName));
    } catch (Exception e) {
      Assert.fail();
    }
  }

  private String jsonFile = "src/test/resources/sample_pipeline_replace.json";


}
