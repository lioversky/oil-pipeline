package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import com.weibo.dip.pipeline.processor.add.FieldAddProcessor;
import com.weibo.dip.pipeline.processor.add.FieldAddTypeEnum;
import com.weibo.dip.pipeline.processor.base64.Base64Processor;
import com.weibo.dip.pipeline.processor.base64.Base64TypeEnum;
import com.weibo.dip.pipeline.processor.converte.ConverteProcessor;
import com.weibo.dip.pipeline.processor.converte.ConverteTypeEnum;
import com.weibo.dip.pipeline.processor.filter.ExprFilterProcessor;
import com.weibo.dip.pipeline.processor.md5.MD5EncodeProcessor;
import com.weibo.dip.pipeline.processor.merge.FieldMergeProcessor;
import com.weibo.dip.pipeline.processor.merge.FieldMergeTypeEnum;
import com.weibo.dip.pipeline.processor.remove.FieldRemoveProcessor;
import com.weibo.dip.pipeline.processor.remove.FieldRemoveTypeEnum;
import com.weibo.dip.pipeline.processor.replace.ReplaceProcessor;
import com.weibo.dip.pipeline.processor.replace.ReplaceTypeEnum;
import com.weibo.dip.pipeline.processor.split.FieldSplitProcessor;
import com.weibo.dip.pipeline.processor.split.FieldSplitTypeEnum;
import com.weibo.dip.pipeline.processor.substring.SubStringProcessor;
import com.weibo.dip.pipeline.processor.substring.SubStringTypeEnum;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * Processor类型生成器.
 * Create by hongxun on 2018/6/27
 */

public enum ProcessorTypeEnum implements TypeEnum {

  /**
   * 字符替换.
   */
  Replace {
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      String fieldName = (String) params.get("fieldName");

      boolean fieldNotExistError = params.containsKey("fieldNotExistError") && (boolean) params
          .get("fieldNotExistError");
      ReplaceTypeEnum replaceTypeEnum = ReplaceTypeEnum.getType(subType);
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new ReplaceProcessor(fieldNotExistError, fieldName,
          replaceTypeEnum.getReplacer(subParams));
    }
  },
  /**
   * 截断
   */
  SubString {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      String fieldName = (String) params.get("fieldName");

      boolean fieldNotExistError = params.containsKey("fieldNotExistError") && (boolean) params
          .get("fieldNotExistError");
      SubStringTypeEnum subStringTypeEnum = SubStringTypeEnum.getType(subType);
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new SubStringProcessor(fieldNotExistError, fieldName,
          subStringTypeEnum.getSubStringer(subParams));
    }
  },
  /**
   * 类型转换
   */
  TypeConverte {
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {

      String subType = (String) params.get("subType");
      String fieldName = (String) params.get("fieldName");

      boolean fieldNotExistError = params.containsKey("fieldNotExistError") && (boolean) params
          .get("fieldNotExistError");
      ConverteTypeEnum converteTypeEnum = ConverteTypeEnum.getType(subType);
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new ConverteProcessor(fieldNotExistError, fieldName,
          converteTypeEnum.getConverter(subParams));
    }
  },
  /**
   * 过滤器
   */
  Filter {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String expr = (String) params.get("expr");
      return new ExprFilterProcessor(expr);
    }
  },
  /**
   * 嵌套打平
   */
  NestingFlat,
  /**
   * base64转码
   */
  Base64 {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String fieldName = (String) params.get("fieldName");
      String subType = (String) params.get("subType");
      boolean fieldNotExistError = params.containsKey("fieldNotExistError") && (boolean) params
          .get("fieldNotExistError");
      Base64TypeEnum base64TypeEnum = Base64TypeEnum.getType(subType);
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new Base64Processor(fieldNotExistError, fieldName,
          base64TypeEnum.getBase64er(subParams));
    }
  },

  /**
   * md5转码
   */
  MD5 {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String fieldName = (String) params.get("fieldName");

      boolean fieldNotExistError = params.containsKey("fieldNotExistError") && (boolean) params
          .get("fieldNotExistError");
      return new MD5EncodeProcessor(fieldNotExistError, fieldName);
    }
  },
  /**
   * 列拆分
   */
  FieldSplit {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String targetField = (String) params.get("targetFields");
      boolean overwriteIfFieldExist =
          !params.containsKey("overwriteIfFieldExist") || (boolean) params
              .get("overwriteIfFieldExist");
      boolean fieldNotExistError = params.containsKey("fieldNotExistError") && (boolean) params
          .get("fieldNotExistError");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      String subType = (String) params.get("subType");
      String fieldName = (String) params.get("fieldName");
      FieldSplitTypeEnum fieldSplitTypeEnum = FieldSplitTypeEnum.getType(subType);

      return new FieldSplitProcessor(StringUtils.split(targetField, ","), fieldName,
          fieldNotExistError, overwriteIfFieldExist, fieldSplitTypeEnum.getSpliter(subParams));
    }
  },
  /**
   * 列合并
   */
  FieldMerge {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String targetField = (String) params.get("targetField");
      boolean overwriteIfFieldExist =
          !params.containsKey("overwriteIfFieldExist") || (boolean) params
              .get("overwriteIfFieldExist");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      String subType = (String) params.get("subType");
      FieldMergeTypeEnum fieldMergeTypeEnum = FieldMergeTypeEnum.getType(subType);
      return new FieldMergeProcessor(targetField,
          overwriteIfFieldExist, fieldMergeTypeEnum.getMerger(subParams));
    }
  },
  /**
   * 列删除
   */
  FieldRemove {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      FieldRemoveTypeEnum removeTypeEnum = FieldRemoveTypeEnum.getType(subType);
      String fields = (String) params.get("fields");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new FieldRemoveProcessor(removeTypeEnum.getFieldRemover(subParams));
    }
  },
  /**
   * 列增加
   */
  FieldAdd {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      FieldAddTypeEnum fieldAddTypeEnum = FieldAddTypeEnum.getType(subType);
      boolean overwriteIfFieldExist =
          params.containsKey("overwriteIfFieldExist") && (boolean) params
              .get("overwriteIfFieldExist");
      String targetField = (String) params.get("targetField");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new FieldAddProcessor(overwriteIfFieldExist, targetField,
          fieldAddTypeEnum.getFieldAdder(subParams));

    }
  },
  /**
   * 列hash
   */
  FieldHash,
  /**
   * 转json
   */
  ToJSON,
  /**
   * 转xml
   */
  ToXML,
  /**
   * 转字符串
   */
  ToString;


  public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
    throw new RuntimeException("Abstract Error!!!");
  }


  private static final Map<String, ProcessorTypeEnum> types =
      new ImmutableMap.Builder<String, ProcessorTypeEnum>()
          .put("processor_replace", Replace)
          .put("processor_substring", SubString)
          .put("processor_converte", TypeConverte)
          .put("processor_filter", Filter)
          .put("processor_nestingflat", NestingFlat)
          .put("processor_base64", Base64)
          .put("processor_md5", MD5)
          .put("processor_fieldsplit", FieldSplit)
          .put("processor_fieldmerge", FieldMerge)
          .put("processor_fieldremove", FieldRemove)
          .put("processor_fieldadd", FieldAdd)
          .put("processor_fieldhash", FieldHash)
          .put("processor_tojson", ToJSON)
          .put("processor_toxml", ToXML)
          .put("processor_tostring", ToString).build();

  public static ProcessorTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
