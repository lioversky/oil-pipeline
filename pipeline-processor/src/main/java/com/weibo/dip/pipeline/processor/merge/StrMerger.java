package com.weibo.dip.pipeline.processor.merge;

import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;

/**
 * 合成字符串
 */
public class StrMerger extends FieldMergeProcessor {

  private String splitStr;

  public StrMerger(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
    if (splitStr == null) {
      throw new AttrCanNotBeNullException("StrMerger splitStr can not be null!!!");
    }
  }

  @Override
  Object merge(Map<String, Object> data) throws Exception {
    StringBuilder sb = new StringBuilder();

    for (String field : fields) {
      if (sb.length() > 0) {
        sb.append(splitStr);
      }
      if (data.containsKey(field)) {
        sb.append(data.get(field).toString());
      }

    }
    return sb.toString();
  }
}
