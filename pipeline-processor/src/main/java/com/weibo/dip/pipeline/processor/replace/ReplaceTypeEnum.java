package com.weibo.dip.pipeline.processor.replace;

import com.google.common.collect.ImmutableMap;
import com.weibo.dip.pipeline.enums.TypeEnum;
import java.util.Map;

/**
 * 列替换处理生成器.
 * Create by hongxun on 2018/6/27
 */

public enum ReplaceTypeEnum implements TypeEnum {
  StrToDate {
    @Override
    public Replacer getReplacer(Map<String, Object> params) {

      return new StrToDateReplacer(params);
    }
  },
  StrToTimestamp {
    @Override
    public Replacer getReplacer(Map<String, Object> params) {
      return new StrToTimestampReplacer(params);
    }
  },
  StrToUnixTimestamp {
    @Override
    public Replacer getReplacer(Map<String, Object> params) {
      return new StrToUnixTimestampReplacer(params);
    }
  },
  UnixToDateStr {
    @Override
    public Replacer getReplacer(Map<String, Object> params) {
      return new UnixToDateStrReplacer(params);
    }
  },
  StrToDateStr {
    @Override
    public Replacer getReplacer(Map<String, Object> params) {

      return new StrToDateStrReplacer(params);
    }
  },
  TimestampToDateStr {
    @Override
    public Replacer getReplacer(Map<String, Object> params) {
      return new TimestampToDateStrReplacer(params);
    }
  },
  Regex {
    @Override
    public Replacer getReplacer(Map<String, Object> params) {

      return new RegexReplacer(params);

    }
  },
  ReplaceStr {
    @Override
    public Replacer getReplacer(Map<String, Object> params) {
      return new ReplaceStrReplacer(params);
    }


  };


  private static final Map<String, ReplaceTypeEnum> types =
      new ImmutableMap.Builder<String, ReplaceTypeEnum>()
          .put("replace_str_date", StrToDate)
          .put("replace_str_timestamp", StrToTimestamp)
          .put("replace_str_unix", StrToUnixTimestamp)
          .put("replace_unix_str", UnixToDateStr)
          .put("replace_str_str", StrToDateStr)
          .put("replace_timestamp_str", TimestampToDateStr)
          .put("replace_regex", Regex)
          .put("replace_replace_str", ReplaceStr).build();

  public Replacer getReplacer(Map<String, Object> params) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static ReplaceTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
