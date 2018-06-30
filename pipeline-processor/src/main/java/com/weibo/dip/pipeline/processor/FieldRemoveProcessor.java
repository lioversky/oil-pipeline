package com.weibo.dip.pipeline.processor;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.weibo.dip.pipeline.configuration.Configuration;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class FieldRemoveProcessor extends Processor {


  private FieldRemover remover;

  public FieldRemoveProcessor(FieldRemover remover) {
    this.remover = remover;
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {

    //    todo: keep 时列不存在处理
    return remover.fieldRemove(data);

  }
}

abstract class FieldRemover extends Configuration {

  String[] fields;

  public FieldRemover(Map<String, Object> parmas) {
    String fields = (String) parmas.get("fields");
    this.fields = StringUtils.split(fields, ",");
    addConfigs(parmas);
  }

  abstract Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception;
}

/**
 * 删除指定字段
 */
class RemoveFieldRemover extends FieldRemover {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    for (String field : fields) {
      data.remove(field);
    }
    return data;
  }

  public RemoveFieldRemover(Map<String, Object> parmas) {
    super(parmas);

  }
}

/**
 * 保留指定字段
 */
class KeepFieldRemover extends FieldRemover {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    Map<String, Object> newData = Maps.newHashMap();
    for (String field : fields) {
      newData.put(field, data.get(field));
    }
    return newData;
  }

  public KeepFieldRemover(Map<String, Object> parmas) {
    super(parmas);

  }
}

/**
 * 删除为空的指定字段
 */
class RemoveNullFieldRemover extends FieldRemover {

  @Override
  Map<String, Object> fieldRemove(Map<String, Object> data) throws Exception {
    for (String field : fields) {
      if (data.containsKey(field) && data.get(field) == null) {
        data.remove(field);
      }
    }
    return data;
  }

  public RemoveNullFieldRemover(Map<String, Object> parmas) {
    super(parmas);

  }
}

