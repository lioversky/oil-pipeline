package com.weibo.dip.pipeline.processor.remove;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.configuration.Configuration;
import com.weibo.dip.pipeline.exception.AttrCanNotBeNullException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Create by hongxun on 2018/7/1
 */
abstract class Remover extends Configuration {

  String[] fields;

  public Remover(Map<String, Object> params) {
    String fields = (String) params.get("fields");
    if (Strings.isNullOrEmpty(fields)) {
      throw new AttrCanNotBeNullException("Remover fields can not be null!!!");
    }
    this.fields = StringUtils.split(fields, ",");
    addConfigs(params);
  }

  abstract Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception;
}

/**
 * 删除指定字段
 */
class RemoveFieldRemover extends Remover {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    for (String field : fields) {
      data.remove(field);
    }
    return data;
  }

  public RemoveFieldRemover(Map<String, Object> params) {
    super(params);

  }
}

/**
 * 保留指定字段
 */
class KeepFieldRemover extends Remover {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    Map<String, Object> newData = Maps.newHashMap();
    for (String field : fields) {
      newData.put(field, data.get(field));
    }
    return newData;
  }

  public KeepFieldRemover(Map<String, Object> params) {
    super(params);

  }
}

/**
 * 删除为空的指定字段
 */
class RemoveNullFieldRemover extends Remover {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    for (String field : fields) {
      if (data.containsKey(field) && data.get(field) == null) {
        data.remove(field);
      }
    }
    return data;
  }

  public RemoveNullFieldRemover(Map<String, Object> params) {
    super(params);

  }
}

