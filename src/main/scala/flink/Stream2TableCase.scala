package flink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
object Stream2TableCase {
  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(streamEnv)
    val dataStream = streamEnv.socketTextStream("localhost",12345)
    val studentStream = dataStream.map(a => {
      val strings = a.split(",")
      Student(strings(0), strings(1).toInt)
    }).keyBy(_.name)

    tableEnv.registerDataStream("table1",studentStream)
//    val table = tableEnv.scan("table1")
    val table1 = tableEnv.sqlQuery(
      """
        |select * from table1 where name = 'py'
      """.stripMargin)
//    table.printSchema()

    table1
      .toAppendStream[Student]
      .print()


    streamEnv.execute("test")

  }
}

case class Student(name:String,age:Int)