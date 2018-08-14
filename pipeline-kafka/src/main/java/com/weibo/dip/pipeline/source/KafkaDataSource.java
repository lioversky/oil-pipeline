package com.weibo.dip.pipeline.source;

import java.util.Map;

/**
 * 读取kafka数据的顶层抽象类.
 * Create by hongxun on 2018/8/1
 */
public abstract class KafkaDataSource extends Source {

  public abstract Object createSource(Map<String, Object> map);
}
