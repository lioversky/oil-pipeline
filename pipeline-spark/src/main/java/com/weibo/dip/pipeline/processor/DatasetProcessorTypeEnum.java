package com.weibo.dip.pipeline.processor;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/18
 */
public enum DatasetProcessorTypeEnum implements TypeEnum {

  Add {
    @Override
    public DatasetProcessor getProcessor(Map<String, Object> params) throws RuntimeException {
      return super.getProcessor(params);
    }
  },
  Convert {

  }, Filter {

  },
  Flatten {

  },
  Merge {

  },
  Remove {

  },
  Replace {

  },
  Split {

  },
  Substring {

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
