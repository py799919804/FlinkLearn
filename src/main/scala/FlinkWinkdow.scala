import java.text.SimpleDateFormat

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector



/*
*
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector
*
* */


object FlinkWinkdow {

  def main(args: Array[String]): Unit = {

    val streamEnv= StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.enableCheckpointing(5000)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 允许的最大checkpoint并行度
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    val hostname = "localhost"
    val port = 9999
    val input = streamEnv.socketTextStream(hostname,port)

    input
      .map(a => {
        val split = a.split(",")
        (split(0), split(1).toLong)
      })
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
        var currentMaxTimestamp = 0L
        val maxOutOfOrderness = 10000L
        //最大允许的乱序时间是10s
        var waterMark: Watermark = null
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        override def getCurrentWatermark: Watermark = {
          waterMark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
          waterMark
        }

        override def extractTimestamp(t: (String, Long), l: Long): Long = {
          val timestamp = t._2
          currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)

          println("timestamp:" + t._1 + "," + t._2 + "|" + format.format(t._2) + "," + currentMaxTimestamp + "|" + format.format(currentMaxTimestamp) + "," + waterMark.toString)
          timestamp
        }
      })
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply[(String, Int,String,String,String,String)](new WindowFunctionTest)
      .print()

//        .apply(new WindowFunctionTest)



    streamEnv.execute("window")


}

}

class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String,String)]): Unit = {
//    val list = input.asInstanceOf[List[(String, Long)]].sortBy(_._2)
    val list = input.toList.sortBy(_._2)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    out.collect(key,list.size,format.format(list.head._2),format.format(list.last._2),format.format(window.getStart),format.format(window.getEnd))
  }
}
