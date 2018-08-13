package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.dip.util.MD5Util;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class ConvertProcessorTest {

  private String jsonFile = "src/test/resources/sample_pipeline_convert.json";

  @Test
  public void testBase64FromJson() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("base64", "base64data"));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      for (Processor<Map<String,Object>> p : processorList) {
        data = p.process(data);
      }
      Assert.assertTrue(data.containsKey("base64"));
      Assert.assertEquals("base64data",  data.get("base64"));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testJsonMd5() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("md5", "mmmmddddd5555"));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      Processor<Map<String,Object>> p = processorList.get(2);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("md5"));
      Assert.assertNotEquals("mmmmddddd5555", data.get("md5"));
      Assert.assertEquals(MD5Util.md5("mmmmddddd5555"), data.get("md5"));

    } catch (Exception e) {
      Assert.fail();
    }
  }


}
