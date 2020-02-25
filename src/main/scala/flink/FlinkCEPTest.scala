package flink
import java.util

import flink_01.MyTrigger
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object FlinkCEPTest {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.enableCheckpointing(1000)
    streamEnv.getConfig.setAutoWatermarkInterval(3000L)
    streamEnv.setParallelism(1)

    val input = streamEnv.socketTextStream("localhost",9999)
    val input2 = input.map(line => {
      val strings = line.split(",")
      Event(strings(0).toInt,strings(1),strings(2),strings(3).toLong)
    })
          .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Event] {
            var currentMaxTimestamp = 0l
            //水印延迟时间
            val DELAY_TIME = 3 * 1000l
            var watermark:Watermark = null
            override def getCurrentWatermark: Watermark = {

              watermark = new Watermark(currentMaxTimestamp - DELAY_TIME)
              watermark
            }
            override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = {
              val timeTmp = element.time
              currentMaxTimestamp = Math.max(timeTmp,currentMaxTimestamp)
              timeTmp
            }
          })
      .keyBy(_.userId)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .trigger(MyTrigger.create())
      .process(new ProcessWindowFunction[Event,Event,Int,TimeWindow] {
        override def process(key: Int, context: Context, elements: Iterable[Event], out: Collector[Event]): Unit = {
          elements.foreach(out.collect(_))
        }
      })

    input2.print()

    val pat = Pattern.begin[Event]("stage1").where(_.userId.equals(97)).or(_.userId == 98).where(_.action.equals("d"))
      .followedBy("stage2").where(_.action.equals("s"))
      .followedBy("stage3").where(_.action.equals("b"))

    val patStream = CEP.pattern(input2,pat)

    patStream.select(new PatternSelectFunction[Event, (String,Int,Long,Long,Long)] {
      override def select(pattern: util.Map[String, util.List[Event]]): (String,Int,Long,Long,Long) = {
        val name = pattern.get("stage1").get(0).userName
        val id = pattern.get("stage1").get(0).userId
        val stage1 = pattern.get("stage1").get(0).time
        val stage2 = pattern.get("stage2").get(0).time
        val stage3 = pattern.get("stage3").get(0).time

        (name,id,stage1,stage2,stage3)
      }
    })
      .print()
//      .keyBy(_._1)
//      .window(SlidingEventTimeWindows.of(Time.seconds(6),Time.seconds(3)))
//      .process(new ProcessWindowFunction[(String, Long, Long, Long),(String, Long, Long, Long),String,TimeWindow] {
//        override def process(key: String, context: Context, elements: Iterable[(String, Long, Long, Long)], out: Collector[(String, Long, Long, Long)]): Unit = {
//          elements.foreach(a => out.collect(a))
//        }
//      } )



    //    Pattern.begin[Event]("start").where(new IterativeCondition[Event] {
    //      override def filter(value: Event, ctx: IterativeCondition.Context[Event]): Boolean = {
    //        value.userId == 97
    //      }
    //    })

    //    val pat = Pattern.begin[Event]("stage1").where(new SimpleCondition[Event] {
    //      override def filter(t: Event): Boolean = {
    //        t.userId == 97
    //      }
    //    }).or(new SimpleCondition[Event] {
    //      override def filter(t: Event): Boolean = {
    //        t.userId == 98
    //      }
    //    }).next("stage2")
    //      .where(new SimpleCondition[Event] {
    //        override def filter(t: Event): Boolean = {
    //          t.action.equals("dianji")
    //        }
    //      })
    //      .next("stage3")
    //      .where(new SimpleCondition[Event] {
    //        override def filter(t: Event): Boolean = {
    //          t.action.equals("shoucang")
    //        }
    //      })
    //      .next("stage4")
    //      .where(new SimpleCondition[Event] {
    //        override def filter(t: Event): Boolean = {
    //          t.action.equals("by")
    //        }
    //      })

    streamEnv.execute("CEPTest")
  }

}
//
//case class Event(val userId:Int, val userName:String, val action:String,val time:Long)