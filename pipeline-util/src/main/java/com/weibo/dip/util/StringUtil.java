package com.weibo.dip.util;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 字符串工具类.
 * Create by hongxun on 2018/7/10
 */
public class StringUtil {

  public static Map<String, String> urlArgsSplit(String urlArgs) {
    return kvSplit(urlArgs, "&", "=");
  }

  /**
   * 解析行为日志扩展字段内容
   * 例：interface_tag=>1_1,spr=>from:1087193010;wm:3333_2001,uids=>5368121613
   * 当值里面包含分号时继续拆分
   *
   * @param extendArgs 行为日志扩展字段
   * @return 拆分成map
   */
  public static Map<String, String> extendArgsSplit(String extendArgs) {
    //spr内容包含外部分隔符，要单独处理
    if (extendArgs.contains("spr=>")) {
      //spr后面有属性
      Pattern p = Pattern.compile("(.*)spr=>(.*),([0-9a-zA-Z]*=>.*)");
      Matcher matcher = p.matcher(extendArgs);
      Map<String, String> resultMap = null;
      if (matcher.find()) {
        //去掉spr的分隔
        resultMap = kvSplit(matcher.group(1) + matcher.group(3), ",", "=>");
        //再分隔spr
        resultMap.putAll(kvSplit(matcher.group(2), ";", ":"));
      } else {
        //spr后面没有属性
        p = Pattern.compile("(.*)spr=>(.*)");
        matcher = p.matcher(extendArgs);
        if (matcher.find()) {
          resultMap = kvSplit(matcher.group(1), ",", "=>");
          resultMap.putAll(kvSplit(matcher.group(2), ";", ":"));
        }
      }
      return resultMap;
    } else {
      return kvSplit(extendArgs, ",", "=>");
    }
  }

  private static Map<String, String> kvSplit(String value, String fieldSplit, String valueSplit) {
    Map<String, String> argsMap = new HashMap<>();
    if (value != null && value.length() != 0) {
      String[] params = value.split(fieldSplit);
      for (int i = 0; i < params.length; i++) {
        String[] p = params[i].split(valueSplit);
        if (p.length == 2) {
          argsMap.put(p[0], p[1]);
        }
      }
    }
    return argsMap;
  }

}
