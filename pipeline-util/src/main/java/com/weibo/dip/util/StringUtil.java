package com.weibo.dip.util;

import java.util.HashMap;
import java.util.Map;

/**
 * Create by hongxun on 2018/7/10
 */
public class StringUtil {

  public static Map<String,String> urlArgsSplit(String urlargs){
    Map<String, String> argsMap = new HashMap<>();
    if (urlargs != null) {
      String[] params = urlargs.split("&");
      for (int i = 0; i < params.length; i++) {
        String[] p = params[i].split("=");
        if (p.length == 2) {
          argsMap.put(p[0], p[1]);
        }
      }
    }
    return argsMap;
  }

}
