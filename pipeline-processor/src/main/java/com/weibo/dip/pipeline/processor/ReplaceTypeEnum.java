package com.weibo.dip.pipeline.processor;

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
    public Replacer getReplacer(Map<String, Object> parmas) {

      return new StrToDateReplacer(parmas);
    }
  },
  StrToTimestamp {
    @Override
    public Replacer getReplacer(Map<String, Object> parmas) {
      return new StrToTimestampReplacer(parmas);
    }
  },
  StrToUnixTimestamp {
    @Override
    public Replacer getReplacer(Map<String, Object> parmas) {
      return new StrToUnixTimestampReplacer(parmas);
    }
  },
  UnixToDateStr {
    @Override
    public Replacer getReplacer(Map<String, Object> parmas) {
      return new UnixToDateStrReplacer(parmas);
    }
  },
  StrToDateStr {
    @Override
    public Replacer getReplacer(Map<String, Object> parmas) {

      return new StrToDateStrReplacer(parmas);
    }
  },
  TimestampToDateStr {
    @Override
    public Replacer getReplacer(Map<String, Object> parmas) {
      return new TimestampToDateStrReplacer(parmas);
    }
  },
  Regex {
    @Override
    public Replacer getReplacer(Map<String, Object> parmas) {

      return new RegexReplacer(parmas);

    }
  },
  ReplaceStr {
    @Override
    public Replacer getReplacer(Map<String, Object> parmas) {
      return new ReplaceStrReplacer(parmas);
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

  public Replacer getReplacer(Map<String, Object> parmas) {
    throw new RuntimeException("Abstract Error!!!");
  }

  public static ReplaceTypeEnum getType(String typeName) {
    return types.get(typeName);
  }
}
