package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.dip.util.GsonUtil;
import com.weibo.dip.util.GsonUtil.GsonType;
import java.io.File;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class Base64ProcessorTest {



  /*@Test
  public void testBase64() {
    Map<String, Object> params = ImmutableMap.of("fieldName", "encode");
    Processor encode = ProcessorTypeEnum.getType("processor_base64encode")
        .getProcessor(params);
    Processor decode = ProcessorTypeEnum.getType("processor_base64decode")
        .getProcessor(params);
    Map<String, Object> data = Maps.newHashMap();
    data.put("encode", "1111".getBytes());
    try {
      Map<String, Object> result = decode.process(encode.process(data));
      Assert.assertEquals("1111", new String((byte[]) result.get("encode")));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }*/

  private String jsonFile = "src/test/resources/sample_pipeline_base64.json";

  @Test
  public void testBase64FromJson() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("base64", "base64data".getBytes()));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      for (Processor p : processorList) {
        data = p.process(data);
      }
      Assert.assertTrue(data.containsKey("base64"));
      Assert.assertEquals("base64data", new String((byte[]) data.get("base64")));
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

}
