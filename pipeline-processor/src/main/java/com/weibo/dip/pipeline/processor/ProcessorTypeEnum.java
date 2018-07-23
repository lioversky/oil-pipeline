package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import com.weibo.dip.pipeline.processor.add.FieldAddProcessor;
import com.weibo.dip.pipeline.processor.add.FieldAddTypeEnum;
import com.weibo.dip.pipeline.processor.base64.Base64Processor;
import com.weibo.dip.pipeline.processor.base64.Base64TypeEnum;
import com.weibo.dip.pipeline.processor.converte.ConvertProcessor;
import com.weibo.dip.pipeline.processor.converte.ConvertTypeEnum;
import com.weibo.dip.pipeline.processor.filter.ExprFilterProcessor;
import com.weibo.dip.pipeline.processor.flatten.FlattenProcessor;
import com.weibo.dip.pipeline.processor.flatten.FlattenerTypeEnum;
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

/**
 * Processor类型生成器.
 * Create by hongxun on 2018/6/27
 */

public enum ProcessorTypeEnum implements TypeEnum {

  /**
   * 字符替换.
   */
  Replace {
    @SuppressWarnings({"unchecked"})
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      ReplaceTypeEnum replaceTypeEnum = ReplaceTypeEnum.getType(subType);
      return new ReplaceProcessor(subParams, replaceTypeEnum.getReplacer(subParams));
    }
  },
  /**
   * 截断
   */
  SubString {
    @SuppressWarnings({"unchecked"})
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      SubStringTypeEnum subStringTypeEnum = SubStringTypeEnum.getType(subType);
      return new SubStringProcessor(subParams, subStringTypeEnum.getSubStringer(subParams));
    }
  },
  /**
   * 类型转换
   */
  TypeConverte {
    @SuppressWarnings({"unchecked"})
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      ConvertTypeEnum convertTypeEnum = ConvertTypeEnum.getType(subType);
      return new ConvertProcessor(subParams, convertTypeEnum.getConverter(subParams));
    }
  },
  /**
   * 过滤器
   */
  Filter {
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new ExprFilterProcessor(subParams);
    }
  },
  /**
   * 嵌套打平
   */
  Flatten {
    @SuppressWarnings({"unchecked"})
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      FlattenerTypeEnum flattenerTypeEnum = FlattenerTypeEnum.getType(subType);
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new FlattenProcessor(subParams, flattenerTypeEnum.getFlattener(subParams));
    }
  },
  /**
   * base64转码
   */
  Base64 {
    @SuppressWarnings({"unchecked"})
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Base64TypeEnum base64TypeEnum = Base64TypeEnum.getType(subType);
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new Base64Processor(subParams, base64TypeEnum.getBase64er(subParams));
    }
  },

  /**
   * md5转码
   */
  MD5 {
    @SuppressWarnings({"unchecked"})
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new MD5EncodeProcessor(subParams);
    }
  },
  /**
   * 列拆分
   */
  FieldSplit {
    @SuppressWarnings({"unchecked"})
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {

      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      String subType = (String) params.get("subType");
      FieldSplitTypeEnum fieldSplitTypeEnum = FieldSplitTypeEnum.getType(subType);
      return new FieldSplitProcessor(subParams, fieldSplitTypeEnum.getSplitter(subParams));
    }
  },
  /**
   * 列合并
   */
  FieldMerge {
    @SuppressWarnings({"unchecked"})
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      String subType = (String) params.get("subType");
      FieldMergeTypeEnum fieldMergeTypeEnum = FieldMergeTypeEnum.getType(subType);
      return new FieldMergeProcessor(subParams, fieldMergeTypeEnum.getMerger(subParams));
    }
  },
  /**
   * 列删除
   */
  FieldRemove {
    @SuppressWarnings({"unchecked"})
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      FieldRemoveTypeEnum removeTypeEnum = FieldRemoveTypeEnum.getType(subType);
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new FieldRemoveProcessor(subParams, removeTypeEnum.getFieldRemover(subParams));
    }
  },
  /**
   * 列增加
   */
  FieldAdd {
    @SuppressWarnings({"unchecked"})
    @Override
    public Processor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      FieldAddTypeEnum fieldAddTypeEnum = FieldAddTypeEnum.getType(subType);
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      return new FieldAddProcessor(subParams, fieldAddTypeEnum.getFieldAdder(subParams));

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
          .put("processor_nestingflat", Flatten)
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
