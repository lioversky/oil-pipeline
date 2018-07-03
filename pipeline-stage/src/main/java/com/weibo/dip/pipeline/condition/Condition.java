package com.weibo.dip.pipeline.condition;

import com.google.common.base.Strings;
import java.util.Map;
import org.nutz.el.El;
import org.nutz.lang.util.Context;
import org.nutz.lang.util.SimpleContext;

/**
 * casewhen判断中条件，返回true或false.
 * Create by hongxun on 2018/6/29
 */
public abstract class Condition {

  protected String expr;

  public Condition(String expr) {
    this.expr = expr;
  }

  public abstract boolean conditional(Map<String, Object> data);

  public static Condition createCondition(Map<String, Object> params) {
    if (params == null || !params.containsKey("expr")) {

      return new OtherwiseCondition(null);
    } else {
      String expr = (String) params.get("expr");
      return new CasewhenCondition(expr);
    }
  }
}

class CasewhenCondition extends Condition {

  public CasewhenCondition(String expr) {
    super(expr);
  }

  @Override
  public boolean conditional(Map<String, Object> data) {
    Context context = new SimpleContext(data);
    //表达式值为假
    return (Boolean) El.eval(context, expr);
  }
}

class OtherwiseCondition extends Condition {

  public OtherwiseCondition(String expr) {
    super(expr);
  }

  @Override
  public boolean conditional(Map<String, Object> data) {
    return true;
  }
}