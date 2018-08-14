package com.weibo.dip.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * 缓存文件key和value
 * Create by hongxun on 2018/8/13
 */
public class FileUtil {

  private static Map<String, Map<String, String>> fileCacheMap = new HashMap<>();

  // todo: guava cache file kv
  public static String getCacheValue(String fileName, String key) {
    if (fileCacheMap.containsKey(fileName)) {
      return fileCacheMap.get(fileName).get(key);
    }
    return null;
  }

  public static void cacheFile(String fileName, String split) {
    InputStream resourceAsStream = FileUtil.class.getClassLoader().getResourceAsStream(fileName);

    BufferedReader reader = null;

    try {
      Map<String, String> map = new HashMap<>();
      fileCacheMap.put(fileName, map);
      reader = new BufferedReader(new InputStreamReader(resourceAsStream));

      String line = null;

      while ((line = reader.readLine()) != null) {
        String[] splits = line.split(split);
        if (splits.length == 2) {
          map.put(splits[0], splits[1]);
        }
      }

    } catch (IOException e) {
      // todo :exception
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {

        }
      }
    }

  }
}
