package com.weibo.dip.pipeline.processor;

import com.google.common.collect.Lists;
import com.weibo.dip.util.GsonUtil;
import com.weibo.dip.util.GsonUtil.GsonType;
import java.util.List;
import java.util.Map;

public class JsonTestUtil {

  public static List<Processor> getProcessors(String jsonFile) throws Exception {
    Map<String, Object> processorsMap = GsonUtil
        .loadJsonFromFile(jsonFile, GsonType.OBJECT_MAP_TYPE);
    List<Map<String, Object>> processorsCofnigList = (List<Map<String, Object>>) processorsMap
        .get("processors");
    List<Processor> processorList = Lists.newArrayList();
    for (Map<String, Object> params : processorsCofnigList) {
      Processor p = ProcessorTypeEnum.getType((String) params.get("processorType"))
          .getProcessor(params);
      processorList.add(p);
    }
    return processorList;

  }
}
