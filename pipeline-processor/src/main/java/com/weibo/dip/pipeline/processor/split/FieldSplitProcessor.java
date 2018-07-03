package com.weibo.dip.pipeline.processor.split;

import com.google.common.collect.Lists;
import com.weibo.dip.pipeline.exception.FieldExistException;
import com.weibo.dip.pipeline.exception.FieldNotExistException;
import com.weibo.dip.pipeline.processor.Processor;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * 列拆分处理器.
 * Create by hongxun on 2018/6/29
 */
public class FieldSplitProcessor extends Processor {


  /**
   * 多匹配
   */
  private List<String[]> multipleTargeFields;
  /**
   *
   */
  private String[] targetFields;
  private String fieldName;
  protected boolean fieldNotExistError;
  private boolean overwriteIfFieldExist;
  private Splitter splitter;

  public FieldSplitProcessor(Map<String, Object> params,
      Splitter splitter) {
    super(params);
    this.splitter = splitter;
    String targetField = (String) params.get("targetFields");

    if (targetField.contains("|")) {
      multipleTargeFields = Lists.newArrayList();
      String[] targets = StringUtils.split(targetField, "|");
      for (String target : targets) {
        multipleTargeFields.add(StringUtils.split(target, ","));
      }
    } else {
      //    目标列，todo: 多匹配，向后依次匹配成功结束
      targetFields = StringUtils.split(targetField, ",");
    }

    overwriteIfFieldExist =
        !params.containsKey("overwriteIfFieldExist") || (boolean) params
            .get("overwriteIfFieldExist");
    fieldNotExistError = params.containsKey("fieldNotExistError") && (boolean) params
        .get("fieldNotExistError");
    fieldName = (String) params.get("fieldName");

  }

  /**
   * 分割成多列.
   *
   * @param data 原始数据
   * @return 分隔后结果
   * @throws Exception 异常
   */
  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {
    //不允许覆盖且目标列存在，抛异常(只单列匹配判断)
    if (!overwriteIfFieldExist && targetFields != null) {
      for (String field : targetFields) {
        if (data.containsKey(field)) {
          throw new FieldExistException(field);
        }
      }
    }
    //列不存在抛异常
    if (fieldNotExistError && !data.containsKey(fieldName)) {
      throw new FieldNotExistException(fieldName);
    }
    //列存在时处理
    if (data.containsKey(fieldName)) {
      Object value = data.get(fieldName);
      if (value != null) {
        Object splitValue = splitter.split(value);
        if (splitValue != null) {
//        数组处理
          if (splitValue instanceof Object[]) {
            Object[] values = (Object[]) splitValue;
            if (values.length > 0) {
              //单匹配时，列数不匹配时异常
              if (targetFields != null) {
                if (targetFields.length != values.length) {
                  throw new RuntimeException(String
                      .format("Split value length %d is not equal to column length %d",
                          values.length,
                          targetFields.length));
                } else {
                  for (int i = 0; i < targetFields.length; i++) {
                    data.put(targetFields[i], values[i]);
                  }
                }
              } else if (multipleTargeFields != null) {
                boolean noMatch = true;
//            匹配后结束，没找到抛异常
                for (String[] targets : multipleTargeFields) {
                  if (targets.length == values.length) {
                    for (int i = 0; i < targets.length; i++) {
                      data.put(targets[i], values[i]);
                    }
                    noMatch = false;
                    break;
                  }
                }
                if (noMatch) {
                  throw new RuntimeException("Split no match fields");
                }
              }

            }
          } else if (splitValue instanceof Map) {
//          Map处理,targetFields必须只能有一列
            if (targetFields == null || targetFields.length != 1) {
              throw new RuntimeException("targetFields must be 1 when result type is Map!!!");
            } else {
              data.put(targetFields[0], splitValue);
            }
          }
        }

      }

    }

    return data;
  }
}

