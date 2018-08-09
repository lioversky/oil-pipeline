package com.weibo.dip.pipeline.processor.replace;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;

/**
 * 替换,接收原字符和目标字符,进行替换
 */
public class ReplaceStrReplacer extends ReplaceProcessor {

  private String source;
  private String target;

  public ReplaceStrReplacer(Map<String, Object> params) {
    super(params);
    this.source = (String) params.get("source");
    if (Strings.isNullOrEmpty(source)) {
      throw new AttrCanNotBeNullException("str replace source can not be null!!!");
    }
    this.target = Strings.nullToEmpty((String) params.get("target"));
    if (target == null) {
      throw new AttrCanNotBeNullException("str replace target can not be null!!!");
    }
  }

  public Object replace(String data) throws Exception {
    return data.replaceAll(source, target);
  }
}
