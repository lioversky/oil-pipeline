package com.weibo.dip.util;

import static com.weibo.dip.util.GsonUtil.GsonType.OBJECT_MAP_TYPE;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.internal.LinkedTreeMap;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class GsonUtil {

  private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(OBJECT_MAP_TYPE, new MapJsonDeserializer()).serializeNulls()
      .disableHtmlEscaping().create();


  public static String toJson(Object object) throws Exception {
    return GSON.toJson(object);
  }

  public static String toJson(Object object, Type type) throws Exception {
    return GSON.toJson(object, type);
  }

  public static <T> T fromJson(String json, Class<T> classOfT) throws Exception {
    return GSON.fromJson(json, classOfT);
  }

  public static <T> T fromJson(String json, Type type) throws Exception {
    return GSON.fromJson(json, type);
  }


  public static class GsonType {

    // primitive
    public static final Type INT_TYPE = new TypeToken<Integer>() {
    }.getType();

    public static final Type DOUBLE_TYPE = new TypeToken<Double>() {
    }.getType();

    public static final Type BOOLEAN_TYPE = new TypeToken<Boolean>() {
    }.getType();

    public static final Type STRING_TYPE = new TypeToken<String>() {
    }.getType();

    // primitive array
    public static final Type INT_ARRAY_TYPE = new TypeToken<int[]>() {
    }.getType();

    public static final Type DOUBLE_ARRAY_TYPE = new TypeToken<double[]>() {
    }.getType();

    public static final Type BOOLEAN_ARRAY_TYPE = new TypeToken<boolean[]>() {
    }.getType();

    public static final Type STRING_ARRAY_TYPE = new TypeToken<String[]>() {
    }.getType();

    // primitive list
    public static final Type INT_LIST_TYPE = new TypeToken<List<Integer>>() {
    }.getType();

    public static final Type DOUBLE_LIST_TYPE = new TypeToken<List<Double>>() {
    }.getType();

    public static final Type BOOLEAN_LIST_TYPE = new TypeToken<List<Boolean>>() {
    }.getType();

    public static final Type STRING_LIST_TYPE = new TypeToken<List<String>>() {
    }.getType();

    // primitive map
    public static final Type INT_MAP_TYPE = new TypeToken<Map<String, Integer>>() {
    }.getType();

    public static final Type DOUBLE_MAP_TYPE = new TypeToken<Map<String, Double>>() {
    }.getType();

    public static final Type BOOLEAN_MAP_TYPE = new TypeToken<Map<String, Boolean>>() {
    }.getType();

    public static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() {
    }.getType();

    // object list
    public static final Type OBJECT_LIST_TYPE = new TypeToken<List<Object>>() {
    }.getType();

    // object map
    public static final Type OBJECT_MAP_TYPE = new TypeToken<Map<String, Object>>() {
    }.getType();

  }

  /**
   * 主要是应对Gson默认将Number转换成Double（会引起精度损失）
   */
  private static class MapJsonDeserializer implements JsonDeserializer<Map<String, Object>> {

    @Override
    public Map<String, Object> deserialize(JsonElement json, Type typeOfT,
        JsonDeserializationContext context) throws JsonParseException {
      return (Map<String, Object>) read(json);
    }

    public Object read(JsonElement in) {

      if (in.isJsonArray()) {
        List<Object> list = new ArrayList<Object>();
        JsonArray arr = in.getAsJsonArray();
        for (JsonElement anArr : arr) {
          list.add(read(anArr));
        }
        return list;
      } else if (in.isJsonObject()) {
        Map<String, Object> map = new LinkedTreeMap<String, Object>();
        JsonObject obj = in.getAsJsonObject();
        Set<Map.Entry<String, JsonElement>> entitySet = obj.entrySet();
        for (Map.Entry<String, JsonElement> entry : entitySet) {
          map.put(entry.getKey(), read(entry.getValue()));
        }
        return map;
      } else if (in.isJsonPrimitive()) {
        JsonPrimitive prim = in.getAsJsonPrimitive();
        if (prim.isBoolean()) {
          return prim.getAsBoolean();
        } else if (prim.isString()) {
          return prim.getAsString();
        } else if (prim.isNumber()) {
          Number num = prim.getAsNumber();

          //保证能够将long型值解析出来
          if (Math.ceil(num.doubleValue()) == num.longValue()) {
            return num.longValue();
          } else {
            return num.doubleValue();
          }
        }
      }
      return null;
    }


  }

  /**
   * 读取文件加载json
   *
   * @param fileName 文件名
   * @param type 转换后类型
   * @param <T> 返回类型
   * @return 返回值
   * @throws Exception 异常
   */
  public static <T> T loadJsonFromFile(String fileName, Type type) throws Exception {

    StringBuilder builder = new StringBuilder();

    BufferedReader reader = null;

    try {

      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));

      String line = null;

      while ((line = reader.readLine()) != null) {
        builder.append(line).append("\n");
      }

    } finally {
      if (reader != null) {
        reader.close();
      }
    }

    return fromJson(builder.toString(), type);
  }
}
