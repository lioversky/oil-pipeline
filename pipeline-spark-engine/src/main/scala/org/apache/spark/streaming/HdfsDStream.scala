package org.apache.spark.streaming

import java.io.{IOException, ObjectInputStream}
import java.text.SimpleDateFormat

//import com.hadoop.mapreduce.LzoTextInputFormat
import com.weibo.dip.pipeline.util.HadoopFileUtil
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{HdfsUnionRDD, RDD, UnionRDD}
import org.apache.spark.streaming.api.java.JavaInputDStream
import org.apache.spark.streaming.dstream.{DStreamCheckpointData, InputDStream}
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.util.Utils

import scala.collection.mutable

/**
  * Created by leilei on 2015/9/21
  */

/**
  * 如果开始时间大于结束时间，则抛出异常
  */


class HdfsDStream(
                   @transient ssc_ : StreamingContext,
                   path: String,
                   categories: Array[String]) extends InputDStream[(Text, Text)](ssc_) {

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
          generatedRDDs += ((t, filesToRDD(t.milliseconds,f)))
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
  private def findNewFiles(currentTime: Long): (Long, Array[String]) = {
    try {
      //    本次读取的文件总大小
      var filesLen: Long = 0l;
      lastNewFileFindingTime = clock.getTimeMillis()
      // Calculate ignore threshold
      // Calculate ignore threshold
      val modTimeIgnoreThreshold = math.max(
        initialModTimeIgnoreThreshold, // initial threshold based on newFilesOnly setting
        currentTime - ssc.graph.batchDuration.milliseconds // trailing end of the remember window
      )
      logDebug(s"Getting new files for time $currentTime, " +
        s"ignoring files older than $modTimeIgnoreThreshold")
      val newFiles = categories.flatMap(c => {
        val list = HadoopFileUtil.getRangeFilePathStatus(modTimeIgnoreThreshold - 5000, currentTime - 5000, c, path, true)
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
    * @param validTime 调用时间，目前没有什么用处
    * @return
    */
  override def compute(validTime: Time): Option[RDD[(Text, Text)]] = {

    val (filesLen, newFiles) = findNewFiles(validTime.milliseconds)

    val fileStr = newFiles.mkString("\n")
    logInfo("New files at time " + validTime + ":\n" + fileStr)
    batchTimeToSelectedFiles += ((validTime, newFiles))
    val metadata = Map(
      "files" -> newFiles,
      StreamInputInfo.METADATA_KEY_DESCRIPTION -> fileStr)
    val inputInfo = StreamInputInfo(id, filesLen / 1024 / 1024, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    Some(filesToRDD(validTime.milliseconds,newFiles))
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

    new HdfsUnionRDD(batchTime,context.sparkContext, allRDDs)
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

object HdfsDStream {

  def apply(
             @transient ssc_ : StreamingContext,
             path: String,
             categories: Array[String]): JavaInputDStream[(Text, Text)] = {
    new HdfsDStream(ssc_, path, categories)
  }

}


