package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import com.weibo.dip.pipeline.processor.add.DatasetAddProcessor;
import com.weibo.dip.pipeline.processor.add.DatasetAddTypeEnum;
import com.weibo.dip.pipeline.processor.convert.DatasetConvertProcessor;
import com.weibo.dip.pipeline.processor.convert.DatasetConvertTypeEnum;
import com.weibo.dip.pipeline.processor.replace.DatasetReplaceProcessor;
import com.weibo.dip.pipeline.processor.replace.DatasetReplaceTypeEnum;
import com.weibo.dip.pipeline.processor.split.DatasetSplitProcessor;
import com.weibo.dip.pipeline.processor.split.DatasetSplitTypeEnum;
import com.weibo.dip.pipeline.processor.substring.DatasetSubstringProcessor;
import com.weibo.dip.pipeline.processor.substring.DatasetSubstringTypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/18
 */
public enum DatasetProcessorTypeEnum implements TypeEnum {

  Add {
    @Override
    public DatasetProcessor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      DatasetAddTypeEnum typeEnum = DatasetAddTypeEnum.getType(subType);
      return new DatasetAddProcessor(subParams, typeEnum.getDatasetAdder(subParams));
    }
  },
  Convert {
    @Override
    public DatasetProcessor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      DatasetConvertTypeEnum typeEnum = DatasetConvertTypeEnum.getType(subType);
      return new DatasetConvertProcessor(subParams, typeEnum.getDatasetConvertor(subParams));
    }
  }, Filter {

  },
  Flatten {

  },
  Merge {

  },
  Remove {

  },
  Replace {
    @Override
    public DatasetProcessor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      DatasetReplaceTypeEnum typeEnum = DatasetReplaceTypeEnum.getType(subType);
      return new DatasetReplaceProcessor(subParams, typeEnum.getDatasetReplacer(subParams));
    }
  },
  Split {
    @Override
    public DatasetProcessor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      DatasetSplitTypeEnum typeEnum = DatasetSplitTypeEnum.getType(subType);
      return new DatasetSplitProcessor(subParams, typeEnum.getDatasetSpliter(subParams));
    }
  },
  Substring {
    @Override
    public DatasetProcessor getProcessor(Map<String, Object> params) throws RuntimeException {
      String subType = (String) params.get("subType");
      Map<String, Object> subParams = (Map<String, Object>) params.get("params");
      DatasetSubstringTypeEnum typeEnum = DatasetSubstringTypeEnum.getType(subType);
      return new DatasetSubstringProcessor(subParams, typeEnum.getDatasetSubstringer(subParams));
    }
  };

  public DatasetProcessor getProcessor(Map<String, Object> params) throws RuntimeException {
    throw new RuntimeException("Abstract Error!!!");
  }

  private static final Map<String, DatasetProcessorTypeEnum> types =
      new ImmutableMap.Builder<String, DatasetProcessorTypeEnum>()
          .put("processor_replace", Replace)
          .put("processor_substring", Substring)
          .put("processor_converte", Convert)
          .put("processor_filter", Filter)
          .put("processor_nestingflat", Flatten)
//          .put("processor_base64", Base64)
//          .put("processor_md5", MD5)
          .put("processor_fieldsplit", Split)
          .put("processor_fieldmerge", Merge)
          .put("processor_fieldremove", Remove)
          .put("processor_fieldadd", Add)
//          .put("processor_fieldhash", FieldHash)
//          .put("processor_tojson", ToJSON)
//          .put("processor_toxml", ToXML)
//          .put("processor_tostring", ToString)
          .build();

  public static DatasetProcessorTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
