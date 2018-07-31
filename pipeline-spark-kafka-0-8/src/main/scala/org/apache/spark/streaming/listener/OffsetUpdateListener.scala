package org.apache.spark.streaming.listener

import com.codahale.metrics.{Gauge, MetricRegistry, Timer}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.Source
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.kafka.OffsetRange
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.KafkaOffsetUtil

import scala.collection.mutable.HashMap

/**
  * Created by hongxun on 2017/7/12.
  */
class OffsetUpdateListener(val consumerGroup: String) extends StreamingListener with Serializable with Logging {

  private var metricRegistry: MetricRegistry = null;
  private val offsetMap = new HashMap[Time, Array[OffsetRange]]()
  var timer: Timer = null
  var gauge: Gauge[Int] = null

  def registry(registry: MetricRegistry): Unit = {
    this.metricRegistry = registry
    timer = metricRegistry.timer(MetricRegistry.name("offsetUpdateTimer"))
    gauge = metricRegistry.register("offsetMap.size", new Gauge[Int] {
      override def getValue: Int = offsetMap.size
    })
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val time = batchCompleted.batchInfo.batchTime
    val opt = offsetMap.get(time)
    if (opt.nonEmpty) {
      val context = timer.time()
      try {
        if (log.isDebugEnabled())
          logDebug(s"update offset at time:$time , ${opt.get.mkString(",")}")
        //    修改此时刻的offset
        KafkaOffsetUtil.updateZKOffsets(opt.get, consumerGroup)
        //    删除所有更早的数据
        val oldTime = offsetMap.filter(_._1 <= time)
        offsetMap --= oldTime.keys
      } finally {
        context.close()
      }

    } else {
      logWarning(s"There is no offset to update at time : $time")
    }
  }

  def addMessage(time: Time, offset: Array[OffsetRange]): Unit = {
    offsetMap.put(time, offset)
  }
}

class OffsetMetricsSources extends Source {
  override val metricRegistry = new MetricRegistry
  override val sourceName = "offset.update"

}