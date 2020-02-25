import java.util.Collections

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

object StateOperatorTest {
  def main(args: Array[String]): Unit = {
    // 定义一个数据类型保存单词出现的次数
    val hostname = "192.168.2.101"
    val port = 9999
    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream(hostname, port, '\n')
    //需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错
    import org.apache.flink.api.scala._
    val windowCounts = text
      .flatMap { line =>
        val p = line.split("\\s+")
        Seq((p(0), p(1).toLong))
      }
      .keyBy(_._1)
//    windowCounts.print()
      //统计各个Key的数量和所有Key的数量总和OperatorState
    .flatMap(new StateFlatMapWithCheckpoint)
    .print().setParallelism(1)
    env.execute("Flink State Test")
  }
}

class StateFlatMapWithCheckpoint extends FlatMapFunction[(String, Long), (String, Long, Long)] with CheckpointedFunction {
  //定义算子实例本地变量，存储Operator 数据数量
  private var operatorCount: Long = _
  //定义KeyState，存储和Key相关的状态值
  private var keyedState: ValueState[Long] = _
  //定义operatorState，存储算子的状态值
  private var operatorState: ListState[Long] = _

  override def flatMap(input: (String, Long), out: Collector[(String, Long, Long)]): Unit = {
    val keyedCount = keyedState.value() + 1
    //更新keyedState数量
    keyedState.update(keyedCount)
    //更新本地算子operatorCount
    //operatorCount = operatorCount + 1
    operatorCount = operatorState.get().asScala.sum + 1
    operatorState.update(Collections.singletonList(operatorCount))
    //输出结果，包括key, key对应的数量统计keyedCount，算子输入的数据量统计operatorCount
    out.collect(input._1, keyedCount, operatorCount)
  }

  //当snapshot发生时，将operatorCount添加到operatorState中
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    operatorState.clear()
    operatorState.add(operatorCount)
  }

  //初始化状态数据
  override def initializeState(context: FunctionInitializationContext): Unit = {
    //定义并获取keyedState
    keyedState = context.getKeyedStateStore.getState(new ValueStateDescriptor[Long]("keyedState", classOf[Long]))
    //定义并获取operatorState
    operatorState = context.getOperatorStateStore.getListState(new ListStateDescriptor[Long]("operatorState", classOf[Long]))
    //定义在Restored过程中，从operatorState中恢复数据的逻辑
    if (context.isRestored) {
      operatorCount = operatorState.get().asScala.sum
    }
  }
}
