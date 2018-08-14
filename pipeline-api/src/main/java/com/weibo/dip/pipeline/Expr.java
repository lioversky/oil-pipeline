package com.weibo.dip.pipeline;

import java.util.Map;

/**
 * 在casewhen中判断条件是否满足，可以与processor组合.
 * Create by hongxun on 2018/6/29
 */
public interface Expr {

  boolean expr(Map<String,Object> data);

}
