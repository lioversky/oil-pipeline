package com.weibo.dip.pipeline.processor.remove;

import com.weibo.dip.pipeline.processor.Processor;
import java.util.Map;

public class FieldRemoveProcessor extends Processor {


  private Remover remover;

  public FieldRemoveProcessor(Map<String, Object> params,
      Remover remover) {
    super(params);
    this.remover = remover;
  }

  public FieldRemoveProcessor(Remover remover) {
    this.remover = remover;
  }

  @Override
  public Map<String, Object> process(Map<String, Object> data) throws Exception {

    //    todo: keep 时列不存在处理
    return remover.fieldRemove(data);

  }
}

