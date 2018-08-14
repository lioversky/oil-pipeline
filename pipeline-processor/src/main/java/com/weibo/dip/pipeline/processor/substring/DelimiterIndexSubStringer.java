package com.weibo.dip.pipeline.processor.substring;

import com.google.common.base.Strings;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import com.weibo.dip.util.StringUtil;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/8/8
 */
public class DelimiterIndexSubStringer extends SubStringProcessor {


  private String delimit;
  private int count;

  public DelimiterIndexSubStringer(Map<String, Object> params) {
    super(params);
    delimit = (String) params.get("delimit");
    if (Strings.isNullOrEmpty(delimit)) {
      throw new AttrCanNotBeNullException(
          "DelimiterIndexSubStringer delimit can not be null!!!");
    }
    count = ((Number) params.get("count")).intValue();
  }

  @Override
  String substring(String value) throws Exception {

    String[] spllits = StringUtils.split(value, delimit);
    if (count >= spllits.length) {
      return value;
    } else {
      StringBuilder sb = new StringBuilder(value.length());
      for (int i = 0; i < count; i++) {
        if (sb.length() > 0) {
          sb.append(delimit);
        }
        sb.append(spllits[i]);
      }
      return sb.toString();
    }

  }

}
