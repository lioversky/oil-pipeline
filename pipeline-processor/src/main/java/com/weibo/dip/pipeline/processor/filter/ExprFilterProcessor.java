package com.weibo.dip.pipeline.processor.filter;

import com.weibo.dip.pipeline.processor.Processor;
import com.weibo.dip.pipeline.processor.StructMapProcessor;
import java.util.Map;

import org.nutz.el.El;
import org.nutz.lang.util.Context;
import org.nutz.lang.util.SimpleContext;

public class ExprFilterProcessor extends StructMapProcessor {

  /**
   * 条件表达式过滤器.
   * Create by hongxun on 2016/6/27
   */
  private String expr;

  public ExprFilterProcessor(Map<String, Object> params) {
    super(params);
    expr = (String) params.get("expr");
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {

    Context context = new SimpleContext(data);
    //表达式值为假
    if (!(Boolean) El.eval(context, expr)) {
      return null;
    }
    return data;
  }


}
