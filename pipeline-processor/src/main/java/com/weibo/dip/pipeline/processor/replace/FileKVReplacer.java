package com.weibo.dip.pipeline.processor.replace;

import com.weibo.dip.util.FileUtil;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/**
 * 自定义文件kv映射替换
 * 定义文件名称分隔符，
 * Create by hongxun on 2018/8/13
 */
public class FileKVReplacer extends ReplaceProcessor {

  private String fileName;
  private String splitStr;
  private String defaultValue;

  public FileKVReplacer(Map<String, Object> params) {
    super(params);
    fileName = (String) params.get("fileName");
    splitStr = (String) params.get("splitStr");
    defaultValue = (String) params.get("defaultValue");
    FileUtil.cacheFile(fileName, splitStr);
  }

  @Override
  Object replace(String value) throws Exception {
    String result = FileUtil.getCacheValue(fileName, value);
    if (StringUtils.isEmpty(result)) {
      return defaultValue;
    } else {
      return result;
    }
  }
}
