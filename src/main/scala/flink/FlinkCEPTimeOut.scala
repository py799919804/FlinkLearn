package flink

import java.util

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

object FlinkCEPTimeOut {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.enableCheckpointing(1000)
    streamEnv.getConfig.setAutoWatermarkInterval(3000L)
    streamEnv.setParallelism(1)


    val source = streamEnv.socketTextStream("localhost",9999)
    val input = source.map(line => {
      val strings = line.split(",")
      (strings(0), strings(1).toLong)
    })
    val pat = Pattern.begin[(String,Long)]("stage1").where(_._1.contains("ab"))
      .followedBy("stage2").where(_._1.contains("cd"))
      .within(Time.seconds(3))

    val patStream = CEP.pattern(input,pat)

    val timeoutPattern = new PatternTimeoutFunction[(String,Long),(String,Long)] {
      override def timeout(pattern: util.Map[String, util.List[(String,Long)]], timeoutTimestamp: Long): (String,Long) = {
        ("no input message within 3 seconds!",timeoutTimestamp)
      }
    }

    val selectPattern = new PatternSelectFunction[(String,Long),(String,String)] {
      override def select(pattern: util.Map[String, util.List[(String,Long)]]): (String,String) = {

//        ("stage1|" + String.join(",",pattern.get("stage1").get(0).) ,
//        "stage2|" + String.join(",",pattern.get("stage2")))
        ("String1|"+pattern.get("stage1").toString,"String2|"+pattern.get("stage2").toString)
      }
    }

    new AssignerWithPeriodicWatermarks[(String,Long)] {

      var watermark = null
      var currentMaxTimestamp = 0L
      val DAILY_TIME = 3 * 1000L

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - DAILY_TIME)
    }

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timeTmp = element._2
        currentMaxTimestamp = Math.max(currentMaxTimestamp,timeTmp)
        currentMaxTimestamp
      }
    }

    patStream.select(timeoutPattern,selectPattern).print()

    streamEnv.execute("TimeoutTest")

  }

}
