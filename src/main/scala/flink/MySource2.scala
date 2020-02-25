package flink

import java.util
import java.util.Random

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MySource2 extends RichSourceFunction[String]{

  private val random = new Random
  private var isRunning = true


  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {

    val nameList = new util.ArrayList[String]()
    nameList.add("py1")
    nameList.add("py2")
    nameList.add("py3")
    nameList.add("py4")

    val actionList = new util.ArrayList[String]()
    actionList.add("点击")
    actionList.add("收藏")
    actionList.add("下单")
    actionList.add("支付")


    while ( {
      isRunning
    }) {
      val num = random.nextInt(nameList.size())
      val userName = nameList.get(num)
      val timestamp = System.currentTimeMillis()
      val action = actionList.get(random.nextInt(actionList.size()))
      val sensorReading = num+1  + "," + userName + "," + action.toString + "," + timestamp.toString
      ctx.collect(sensorReading)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    this.isRunning = false
  }
}
