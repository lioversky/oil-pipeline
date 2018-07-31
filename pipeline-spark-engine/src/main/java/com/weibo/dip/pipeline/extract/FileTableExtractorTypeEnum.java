package com.weibo.dip.pipeline.extract;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/25
 */
public enum FileTableExtractorTypeEnum implements TypeEnum {

  Csv {
    @Override
    public FileTableExtractor getExtractor(Map<String, Object> params) {
      return new CsvTableExtractor(params);
    }
  },
  Delimiter {
    @Override
    public FileTableExtractor getExtractor(Map<String, Object> params) {
      return new DelimiterTableExtractor(params);
    }
  },
  Regex {
    @Override
    public FileTableExtractor getExtractor(Map<String, Object> params) {
      return new RegexTableExtractor(params);
    }
  },
  Table {
    @Override
    public FileTableExtractor getExtractor(Map<String, Object> params) {
      return new SparkTableExtractor(params);
    }
  },
  Schema {
    @Override
    public FileTableExtractor getExtractor(Map<String, Object> params) {
      return new SchemaTableExtractor(params);
    }
  };
  private static final Map<String, FileTableExtractorTypeEnum> types =
      new ImmutableMap.Builder<String, FileTableExtractorTypeEnum>()
          .put("text", Delimiter)
          .put("regex", Regex)
          .put("csv", Csv)
          .put("schema", Schema)
          .put("table", Table)
          .build();

  public FileTableExtractor getExtractor(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static FileTableExtractor getType(String typeName, Map<String, Object> params) {
    return types.get(typeName).getExtractor(params);
  }
}
