package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.weibo.dip.util.MD5Util;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class MD5EncodeProcessorTest {

  Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("md5", "1111"));

  public void testMD5() {
    Map<String, Object> params = ImmutableMap.of("fieldName", "md5");
    Processor md5 = ProcessorTypeEnum.getType("processor_md5")
        .getProcessor(params);

    try {
      Map<String, Object> result = md5.process(data);
      Assert.assertEquals(MD5Util.md5("1111"), result.get("md5"));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private String jsonFile = "src/test/resources/sample_pipeline_md5.json";

  @Test
  public void testJsonMd5() {
    Map<String, Object> data = Maps.newHashMap(ImmutableMap.of("md5", "mmmmddddd5555"));
    try {
      List<Processor> processorList = JsonTestUtil.getProcessors(jsonFile);

      Processor p = processorList.get(0);
      data = p.process(data);
      Assert.assertTrue(data.containsKey("md5"));
      Assert.assertNotEquals("mmmmddddd5555", data.get("md5"));

    } catch (Exception e) {
      Assert.fail();
    }
  }


}
