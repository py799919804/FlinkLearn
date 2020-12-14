import java.net.{InetAddress, InetSocketAddress}
import java.util

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.elasticsearch.util.IgnoringFailureHandler
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //source
//    val inputStream  = env.readTextFile("D:/sensor1.txt")
//    val inputStream  = env.socketTextStream("192.168.2.101",9999)
    val inputStream  = env.addSource(new MySource)

    //transform
    import org.apache.flink.api.scala._
    val dataStream = inputStream.map(x => {
      val arr = x.split(",")
      SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
    })

    //sink
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("192.168.2.101", 9200))
    httpHosts.add(new HttpHost("192.168.2.102", 9200))
    httpHosts.add(new HttpHost("192.168.2.103", 9200))
    //创建一个esSink的Builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading] (
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          print("saving data " + t)
          //包装成一个Map或者JsonObject
          val hashMap = new util.HashMap[String, String]()
          hashMap.put("sensor_id", t.id)
          hashMap.put("temperature", t.temperature.toString)
          hashMap.put("timestamp", t.timestamp.toString)
          //创建index request,准备发送数据
          val indexRequest = Requests.indexRequest().index("sensor").`type`("readingData").source(hashMap)
          //发送请求,写入数据
          requestIndexer.add(indexRequest)
          println("data saved successfully")
        }
      }
    )

    dataStream.addSink(esSinkBuilder.build())

    env.execute("es sink test")

  }
}

case class SensorReading(id: String, timestamp: Long, temperature: Double)