package org.apache.spark.streaming

import java.io.{IOException, ObjectInputStream}
import java.text.SimpleDateFormat
import java.util.{Date, Timer, TimerTask}

//import com.hadoop.mapreduce.LzoTextInputFormat
import com.weibo.dip.pipeline.util.HadoopFileUtil
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.{HdfsUnionRDD, RDD}
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
  * Created by hongxun
  */

/**
  * 记录上次最后执行的Time，从该时间恢复数据
  */


class HdfsTimeDStream(
                       @transient ssc_ : StreamingContext,
                       path: String,
                       categories: Array[String],
                       baseTime: Long,
                       multiple: Int) extends InputDStream[(Text, Text)](ssc_) {

  var currentDealTime: Long = baseTime

  private var isUsedCurrentTime: Boolean = {
    if (baseTime != null) {
      false
    } else true
  }

  // This is a def so that it works during checkpoint recovery:
  private def clock = ssc.scheduler.clock

  // Initial ignore threshold based on which old, existing files in the directory (at the time of
  // starting the streaming application) will be ignored or considered
  private val initialModTimeIgnoreThreshold = clock.getTimeMillis()

  // Timestamp of the last round of finding files
  @transient private var lastNewFileFindingTime = 0L

  private val timeFormat: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")

  // Map of batch-time to selected file info for the remembered batches
  // This is a concurrent map because it's also accessed in unit tests
  @transient private[streaming] var batchTimeToSelectedFiles =
  new mutable.HashMap[Time, Array[String]] with mutable.SynchronizedMap[Time, Array[String]]


  // Data to be saved as part of the streaming checkpoints
  protected[streaming] override val checkpointData = new HttpHdfsDStreamCheckpointData

  private[streaming]
  class HttpHdfsDStreamCheckpointData extends DStreamCheckpointData(this) {

    private def hadoopFiles = data.asInstanceOf[mutable.HashMap[Time, Array[String]]]

    override def update(time: Time) {
      logInfo(s"HttpHdfsDStreamCheckpointData hadoopFiles::${hadoopFiles}")
      hadoopFiles.clear()
      hadoopFiles ++= batchTimeToSelectedFiles

    }

    override def cleanup(time: Time) {}

    override def restore(): Unit = {
      hadoopFiles.toSeq.sortBy(_._1)(Time.ordering).foreach {
        case (t, f) => {
          // Restore the metadata in both files and generatedRDDs
          logInfo("Restoring files for time " + t + " - " +
            f.mkString("[", ", ", "]"))
          batchTimeToSelectedFiles += ((t, f))
          generatedRDDs += ((t, filesToRDD(t.milliseconds, f)))
        }
      }

    }

    override def toString: String = {
      "[\n" + hadoopFiles.size + " file sets\n" +
        hadoopFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n") + "\n]"
    }
  }


  override def start(): Unit = {}

  override def stop(): Unit = {}

  /**
    * Find new files for the batch of `currentTime`. This is done by first calculating the
    * ignore threshold for file mod times, and then getting a list of files filtered based on
    * the current batch time and the ignore threshold. The ignore threshold is the max of
    * initial ignore threshold and the trailing end of the remember window (that is, which ever
    * is later in time).
    */
  private def findNewFiles(startTime: Long, endTime: Long): (Long, Array[String]) = {
    try {
      //    本次读取的文件总大小
      var filesLen: Long = 0l;
      lastNewFileFindingTime = clock.getTimeMillis()
      // Calculate ignore threshold
      // Calculate ignore threshold

      logDebug(s"Getting new files for time $endTime, " +
        s"ignoring files older than $startTime")
      val newFiles = categories.flatMap(c => {
        val list = HadoopFileUtil.getRangeFilePathStatus(startTime - 5000, endTime - 5000, c, path, true)
        import scala.collection.JavaConversions._
        list.map(filePathStatus => {
          val simplePath = filePathStatus.getPath.toString
          val filess = simplePath.split("/")
          val re = new StringBuffer

          for (i <- 3 to filess.length - 1) {
            re.append("/").append(filess(i))
          }
          if (!re.toString.contains("_COPYING_")) {
            filesLen = filesLen + filePathStatus.getLen
          }
          re.toString
        })
      }).filter(!_.contains("_COPYING_")).filter(!_.startsWith("."))

      val timeTaken = clock.getTimeMillis() - lastNewFileFindingTime
      logInfo("Finding new files took " + timeTaken + " ms")
      if (timeTaken > slideDuration.milliseconds) {
        logWarning(
          "Time taken to find new files exceeds the batch size. " +
            "Consider increasing the batch size or reducing the number of " +
            "files in the monitored directory."
        )
      }
      (filesLen, newFiles)
    } catch {
      case e: Exception =>
        logWarning("Error finding new files", e)
        (0l, Array.empty)
    }
  }


  /**
    * 重写InputDString方法，用于获取最新文件
    *
    * 如果非处理当前时间，给findNewFiles
    *
    * @param validTime 调用时间，目前没有什么用处
    * @return
    */
  override def compute(validTime: Time): Option[RDD[(Text, Text)]] = {
    //    ssc.scheduler.submitJobSet(JobSet(validTime, graph.generateJobs(validTime)))
    val (startTime, endTime) = {
      var sTime: Long = validTime.milliseconds
      var eTime: Long = validTime.milliseconds
      if (isUsedCurrentTime) {
        sTime = math.max(
          initialModTimeIgnoreThreshold, // initial threshold based on newFilesOnly setting
          validTime.milliseconds - ssc.graph.batchDuration.milliseconds // trailing end of the remember window
        )
      } else {
        sTime = currentDealTime
        if (validTime.milliseconds <= currentDealTime + ssc.graph.batchDuration.milliseconds * multiple) {
          isUsedCurrentTime = true
          currentDealTime = validTime.milliseconds
        } else {
          eTime = currentDealTime + ssc.graph.batchDuration.milliseconds * multiple
          currentDealTime = eTime
        }
      }
      (sTime, eTime)
    }

    val (filesLen, newFiles) = findNewFiles(startTime, endTime)
    val fileStr = "Find file from time: " + timeFormat.format(new Date(startTime)) + " ,to time: " + timeFormat.format(new Date(endTime)) + " \n" + newFiles.mkString("\n")
    logInfo(fileStr)
    batchTimeToSelectedFiles += ((new Time(validTime.milliseconds), newFiles))
    val metadata = Map(
      "files" -> newFiles,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> fileStr)
    val inputInfo = StreamInputInfo(id, filesLen / 1024 / 1024, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    Some(filesToRDD(endTime, newFiles))
  }

  def filesToRDD(batchTime: Long, newFiles: Array[String]): RDD[(Text, Text)] = {

    val resultRdd = newFiles.filter(file => {
      //过滤不存在的文件
      HadoopFileUtil.exists(file)

    }).map(file => {


      val fullName = file

      //     生成 hadoop file rdd
//      if (file.endsWith(".lzo"))
//        context.sparkContext.newAPIHadoopFile[LongWritable, Text, LzoTextInputFormat](fullName)
//      else
        context.sparkContext.newAPIHadoopFile[LongWritable, Text, TextInputFormat](fullName)

    })
    val allRDDs = newFiles.zip(resultRdd).map({ case (file, rdd) => {
      //      只保留文件名称
      val fileName = file.split('/')
      rdd.map(record => (record._2, new Text(fileName(fileName.length - 4))))
    }
    })

    new HdfsUnionRDD(batchTime, context.sparkContext, allRDDs)
  }


  @throws(classOf[IOException])
  private def readObject(ois: ObjectInputStream): Unit = Utils.tryOrIOException {
    logDebug(this.getClass().getSimpleName + ".readObject used")
    ois.defaultReadObject()
    generatedRDDs = new mutable.HashMap[Time, RDD[(Text, Text)]]()
    batchTimeToSelectedFiles =
      new mutable.HashMap[Time, Array[String]] with mutable.SynchronizedMap[Time, Array[String]]

  }

  /** Clear the old time-to-files mappings along with old RDDs */
  protected[streaming] override def clearMetadata(time: Time) {
    super.clearMetadata(time)
    val oldFiles = batchTimeToSelectedFiles.filter(_._1 < (time - rememberDuration))
    batchTimeToSelectedFiles --= oldFiles.keys
    logInfo("Cleared " + oldFiles.size + " old files that were older than " +
      (time - rememberDuration) + ": " + oldFiles.keys.mkString(", "))
    logDebug("Cleared files are:\n" +
      oldFiles.map(p => (p._1, p._2.mkString(", "))).mkString("\n"))
    // Delete file mod times that weren't accessed in the last round of getting new files
  }

}

object HdfsTimeDStream {
  val categories: List[String] = List("app_picserversweibof6vwt_cachel2ha")

  def main(args: Array[String]): Unit = {
    var currentDealTime = new Time(System.currentTimeMillis() - 10 * 60 * 1000)

    val timer = new Timer()
    timer.schedule(new TimerTask {
      override def run() = {
        val validTime = new Time(System.currentTimeMillis())

        val (startTime, endTime) = {
          var sTime: Long = validTime.milliseconds
          var eTime: Long = validTime.milliseconds

          sTime = currentDealTime.milliseconds
          if (validTime.milliseconds <= currentDealTime.milliseconds + 10000 * 6) {
            currentDealTime = validTime
          } else {
            eTime = currentDealTime.milliseconds + 10000 * 6
            currentDealTime = new Time(eTime)
          }
          (sTime, eTime)
        }
        println(new Date(startTime) + "===" + new Date(endTime))
        findNewFiles(startTime, endTime)
      }
    }, 0l, 10000)

  }

  def findNewFiles(startTime: Long, endTime: Long) {
    try {
      //    本次读取的文件总大小
      var filesLen: Long = 0l;
      // Calculate ignore threshold
      // Calculate ignore threshold

      val newFiles = categories.flatMap(c => {
        val list = HadoopFileUtil.getRangeFilePathStatus(startTime - 5000, endTime - 5000, c, "/user/hdfs/rawlog", true)
        import scala.collection.JavaConversions._
        list.map(filePathStatus => {
          val simplePath = filePathStatus.getPath.toString
          val filess = simplePath.split("/")
          val re = new StringBuffer

          for (i <- 3 to filess.length - 1) {
            re.append("/").append(filess(i))
          }
          if (!re.toString.contains("_COPYING_")) {
            filesLen = filesLen + filePathStatus.getLen
          }
          re.toString
        })
      }).filter(!_.contains("_COPYING_"))

      println(newFiles)
    } catch {
      case e: Exception =>

        (0l, Array.empty)
    }
  }

  def apply(
             @transient ssc_ : StreamingContext,
             path: String,
             categories: Array[String],
             baseTime: Long, multiple: Int): JavaInputDStream[(Text, Text)] = {
    new HdfsTimeDStream(ssc_, path, categories, baseTime, multiple)
  }
}

