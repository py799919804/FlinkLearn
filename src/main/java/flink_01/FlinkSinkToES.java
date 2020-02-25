//package flink_01;
//
//import org.apache.flink.api.common.functions.RuntimeContext;
//import org.apache.flink.streaming.api.scala.DataStream;
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
//import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
//import org.apache.flink.table.descriptors.Elasticsearch;
//import org.elasticsearch.action.index.IndexRequest;
//import org.elasticsearch.client.Requests;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.net.InetAddress;
//import java.net.InetSocketAddress;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class FlinkSinkToES {
//    private static final Logger log = LoggerFactory.getLogger(FlinkSinkToES.class);
//
//    private static final String READ_TOPIC = "student";
//
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        Map<String, String> config = new HashMap<>();
//        config.put("cluster.name", "my-cluster-name");
////该配置表示批量写入ES时的记录条数
//        config.put("bulk.flush.max.actions", "1");
//
//        List<InetSocketAddress> transportAddresses = new ArrayList<>();
//        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.2.101"), 9300));
//        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.2.102"), 9300));
//
//        DataStream<String> input = env.readTextFile("D:/");
//
//        input.addSink(new ElasticsearchSink(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
//            public IndexRequest createIndexRequest(String element) {
//                Map<String, String> json = new HashMap<>();
//                //将需要写入ES的字段依次添加到Map当中
//                json.put("data", element);
//
//                return Requests.indexRequest()
//                        .index("my-index")
//                        .type("my-type")
//                        .source(json);
//            }
//
//            @Override
//            public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
//                indexer.add(createIndexRequest(element));
//            }
//        }));
//        new ElasticsearchSink<>();
//        env.execute("flink learning connectors kafka");
//    }
//}
