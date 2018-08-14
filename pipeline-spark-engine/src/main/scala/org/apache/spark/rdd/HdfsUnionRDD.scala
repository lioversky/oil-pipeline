package org.apache.spark.rdd

import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext

/**
  * Created by hongxun on 17/4/14.
  */
class HdfsUnionRDD(var batchTime: Long, sc: SparkContext, rdds: Seq[RDD[(Text, Text)]]) extends UnionRDD(sc, rdds) {

}
