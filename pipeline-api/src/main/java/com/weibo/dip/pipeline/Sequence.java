package com.weibo.dip.pipeline;

import java.io.Serializable;

/**
 * 生成名称序列的接口.
 *
 * Create by hongxun on 2018/8/11
 */
public interface Sequence extends Serializable {

  int getSequence();
}
