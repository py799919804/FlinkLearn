//import java.net.{InetAddress, InetSocketAddress}
//import java.util
//
//import org.apache.flink.api.common.functions.RuntimeContext
//import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
//import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//
//import scala.collection.mutable
//object ElasticSearchTest01 {
//
//
//  def main(args: Array[String]): Unit = {
//
//    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val stream = streamEnv.addSource(new MySource)
//
//    val config = new mutable.HashMap[String,String]()
//    config.put("cluster.name", "elasticsearch")
//    config.put("bulk.flush.max.actions", "1")
//
//    val addresses = new util.ArrayList[InetSocketAddress]()
//    addresses.add(new InetSocketAddress(InetAddress.getByName("192.168.2.101"), 9300))
//    addresses.add(new InetSocketAddress(InetAddress.getByName("192.168.2.102"), 9300))
//    addresses.add(new InetSocketAddress(InetAddress.getByName("192.168.2.103"), 9300))
//
//
//
//    val sink = new ElasticsearchSink[String](config,addresses,new ElasticsearchSinkFunction[String] {
//      def createIndexRequest(element: String): IndexRequest = {
//        val json = new mutable.HashMap[String,String]()
//        json.put("data", element)
//        //        log.info("data:" + element)
//        Requests.indexRequest
//          .index("my-index-student-0211")
//          .`type`("my-type")
//          .source(json)
//      }
//      override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
//        indexer.add(createIndexRequest(element))
//      }
//    })
//
//
////    stream.map(a => a).addSink()
//
//
//
//  }
//}
