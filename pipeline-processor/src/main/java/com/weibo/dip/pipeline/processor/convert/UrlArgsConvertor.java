package com.weibo.dip.pipeline.processor.convert;

import com.google.common.collect.Maps;
import com.weibo.dip.util.StringUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/8/8
 */
public class UrlArgsConvertor extends ConvertProcessor {

  /**
   * 保留字段
   */
  private List<String> keepFields;

  public UrlArgsConvertor(Map<String, Object> params) {
    super(params);
    String fields = (String) params.get("keepFields");
    if (StringUtils.isNotEmpty(fields)) {
      keepFields = Arrays.asList(StringUtils.split(fields, ","));
    }
  }

  @Override
  public Object converte(Object object) {
    Map<String, String> resultMap = Maps.newHashMap();
    String urlargs = (String) object;
    Map<String, String> argsMap = StringUtil.urlArgsSplit(urlargs);
    for (String field : keepFields) {
      if (argsMap.containsKey(field)) {
        resultMap.put(field, argsMap.get(field));
      }
    }
    return resultMap;
  }
}

