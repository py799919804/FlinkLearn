package flink
import flink_01.MyTrigger
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

//
//import org.apache.flink.api.scala._
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.table.api.scala._
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.cep.CEP
//import org.apache.flink.cep.pattern.Pattern
//import org.apache.flink.cep.pattern.conditions.{IterativeCondition, SimpleCondition}
//import org.apache.flink.streaming.api.windowing.time.Time

object FlinkStreamTest {

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.enableCheckpointing(1000)
    streamEnv.getConfig.setAutoWatermarkInterval(3L)
    streamEnv.setParallelism(1)

//    val input = streamEnv.socketTextStream("192.168.2.101",9999)
    val input = streamEnv.addSource(new MySource2)
    val input2 = input.map(line => {
      val strings = line.split(",")
      Event(strings(0).toInt,strings(1),strings(2),strings(3).toLong)
    })
      /*
      * AssignerWithPeriodicWatermarks;   周期性产生watermark
      * 在实际的生产中Periodic的方式必须结合时间和积累条数两个维度继续周期性产生Watermark，否则在极端情况下会有很大的延时。
      *
      * AssignerWithPunctuatedWatermarks;  Punctuated：不间断产生
      * 在实际的生产中Punctuated方式在TPS很高的场景下会产生大量的Watermark在一定程度上对下游算子造成压力，
      * 所以只有在实时性要求非常高的场景才会选择Punctuated的方式进行Watermark的生成。
      * */
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[Event] {
        var currentMaxTimestamp = 0l
        //水印延迟时间
        val DELAY_TIME = 3 * 1000l
        var watermark:Watermark = null
        override def getCurrentWatermark: Watermark = {
          watermark = new Watermark(currentMaxTimestamp - DELAY_TIME)
//          println("getCurrentWatermark . watermark : " + watermark.getTimestamp)
          watermark
        }
        override def extractTimestamp(element: Event, previousElementTimestamp: Long): Long = {
          val timeTmp = element.time
          currentMaxTimestamp = Math.max(timeTmp,currentMaxTimestamp)
          timeTmp
        }
      })
    input2
      /*
      * windowAll与keyBy.*Window
      * windowAll 是对所有数据设置一个大的窗口
      * keyBy.*Window 是按key分组后，分别对每个key设置一个小的窗口
      * */
      .keyBy(_.userId)
      /*
      * SlidingEventTimeWindows是基于时间时间的滑动窗口
      * TumblingEventTimeWindows为基于时间时间的滚动窗口
      */
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      /*
      * 此处添加自定义的trigger
      * 基于时间时间的默认触发器为EventTimeTrigger
      * */
      .trigger(MyTrigger.create())
      /*
      * 全量聚合：
      * 简单点说是等属于窗口的数据到齐之后，才开始进行聚合计算；即全量聚合在未触发之前，会保存之前的状态，在最后窗口触发时，才会进行计算
      *（所以全量聚合的压力会很大。）
      * 常见的窗口函数：
      * apply(WindowFunction) --- 不过1.3之后被弃用
      * process(processWindowFunction)
      *
      * 增量聚合：
      * 窗口每进入一条数据，就进行一次计算。
      *
      * reduce(reduceFunction)；
      * fold；
      * aggregate(aggregateFunction)；
      * sum(key)；min(key)；max(key)
      * sumBy(key)；minBy(key)；maxBy(key)
      * */
      .process(new ProcessWindowFunction[Event,Event,Int,TimeWindow] {
      override def process(key: Int, context: Context, elements: Iterable[Event], out: Collector[Event]): Unit = {
        elements.foreach(out.collect(_))
      }
    })
      .print()

    streamEnv.execute("FlinkStreamTest")
  }

}

case class Event(val userId:Int, val userName:String, val action:String,val time:Long)

