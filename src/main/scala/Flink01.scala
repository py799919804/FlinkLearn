
import java.util.Properties

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic, scala}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
object Flink01 {

  def main(args: Array[String]): Unit = {

    val steamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment


//    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
//    val environment1: StreamTableEnvironment = StreamTableEnvironment.create(steamEnv,settings)
//    val environment2: TableEnvironment = TableEnvironment.create(settings)
//
//    environment2.scan().

//    val environment = TableEnvironment.getTableEnvironment(steamEnv)
//    val value1: DataSet[Int] = batchEnv.fromCollection(List(1,2,3,4))


//    val v1: scala.DataStream[String] = value.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[String] {
//      override def getCurrentWatermark: Watermark = ???
//
//      override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = ???
//    })
    //设置为事件时间
    steamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val dataStream = steamEnv.fromElements(("a",3),("d",4),("c",2),("c",5),("a",5))
    val mapStream: scala.DataStream[(String, Int)] = dataStream.map(t => (t._1, t._2 + 1))
    //MapFunction操作
//    mapStream.map(new MapFunction[(String,Int),(String,Int)] {
//      override def map(t: (String, Int)): (String, Int) = {
//        (t._1,t._2 + 1)
//      }
//    })
//      .keyBy(_._1)
////      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//
//    mapStream.print()

//    steamEnv.execute("SourceTest")

    val dataSet1 = batchEnv.fromCollection(List((1,2.1),(1,2.1),(1,2.1)))
    dataSet1
//    val dataSet2 = batchEnv.fromElements(Point("a", 10.0), Point("b", 20.0), Point("a", 30.0))
//    dataSet2.map(t => (t.x, t.y)).withForwardedFields("_0")
//      .map{x => (x._1,1L)}
//      .print()

    val value:DataStream[String] = steamEnv.socketTextStream("hadoop01",9999)
    val value2 = value.map((_, 1))
      .keyBy(_._1)
      //        .window()
      .timeWindow(Time.seconds(5))
      .sum(1)
    value
      .keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))

//      .<windowed transformation>(<window function>)
//      .map(_._1)

//    value2.addSink(new FlinkKafkaProducer010("","",))
//
//    steamEnv.execute("test1")
//
//    steamEnv.readTextFile()
//    steamEnv.readFile()
//
//    steamEnv.generateSequence()
//
//    steamEnv.fromCollection()
//    steamEnv.fromElements()
//    steamEnv.fromParallelCollection()
//
//    steamEnv.socketTextStream()
//
//    steamEnv.addSource()
//
//
//    batchEnv.readFileOfPrimitives()
//    batchEnv.readFile()
//    batchEnv.readCsvFile()
//    batchEnv.readTextFileWithValue()
//    batchEnv.readTextFile()
//
//    batchEnv.fromCollection()
//    batchEnv.fromElements()
//    batchEnv.fromParallelCollection()
//    batchEnv.
//    batchEnv.generateSequence()


    //创建流执行环境
    val dataStreamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置基于事件时间处理
    dataStreamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 5秒启动一次checkpoint
    dataStreamEnv.enableCheckpointing(5000)
    // 设置checkpoint只checkpoint一次
    dataStreamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置两次checkpoint的最小时间间隔
    dataStreamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // checkpoint超时的时长
    dataStreamEnv.getCheckpointConfig.setCheckpointTimeout(60000)
    // 允许的最大checkpoint并行度
    dataStreamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 当程序关闭的时，触发额外的checkpoint
    dataStreamEnv.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val properties = new Properties()
    //kafka的节点的IP或者hostName，多个使用逗号分隔
    properties.setProperty("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    //zookeeper的节点的IP或者hostName，多个使用逗号进行分隔
    properties.setProperty("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
    //flink consumer flink的消费者的group.id
    properties.setProperty("group.id", "test-consumer-group");

    // 构建消费者对象
    val consumer: FlinkKafkaConsumer010[String] = new FlinkKafkaConsumer010[String]("kafka_test", new SimpleStringSchema(), properties)

    // 让该消费者对象，作为Source
    val stream = dataStreamEnv.addSource(consumer).print()

    //对stream进行transformation操作

    dataStreamEnv.execute("DataStream_Kafka")


  }
  case class Point(var x: String = "", var y: Double = 0)
}

