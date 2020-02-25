import org.apache.flink.api.scala.{ExecutionEnvironment, _}

object Flink02 {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.readTextFile("D:/test1.txt")

    val counts = text.flatMap{ _.toLowerCase.split(",") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)
    //text格式输出
    // counts.writeAsText("D:/text8.txt",WriteMode.OVERWRITE).setParallelism(1)
    //CSV格式输出，字段分隔符为一个空格，行分隔符为换行符
    counts.writeAsCsv("D:/output", "\n", " ").setParallelism(1)

    //提交任务
    env.execute("DataSetTest")

  }
}
