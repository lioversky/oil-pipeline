package com.weibo.dip.pipeline.processor.split;

import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;

/**
 * 字符串拆分
 */
public class DelimiterSplitter extends FieldSplitProcessor {

  private String splitStr;

  public DelimiterSplitter(Map<String, Object> params) {
    super(params);
    splitStr = (String) params.get("splitStr");
    if (splitStr == null) {
      throw new AttrCanNotBeNullException("DelimiterSplitter splitStr can not be null!!!");
    }
  }

  @Override
  public Object split(Object value) throws Exception {
    //    return StringUtils.split((String)value,splitStr);
    return ((String) value).split(splitStr);
  }
}
