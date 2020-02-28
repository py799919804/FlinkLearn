package flink.streaming.elasticsearch;

//import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author caijinpeng
 * @Titile
 * @Description
 * @date 2020/2/7 16:57
 */
public class FlinkToElasticsearch5 {

    private static final Logger loggr = LoggerFactory.getLogger(FlinkToElasticsearch5.class);

    private static final String INDEX = "pm_raw_p_flink_200200"; // index name
    private static final String INDEX_TYPE = "sigmac"; // document type

//    private static final IdGenerator idGen = IdGenerator.getInstance();
//
//    public static ElasticsearchSink<BizData> getElasticsearchSink(String esTransPorts, String clusterName, int maxActions) {
//        if(maxActions < 5000){
//            maxActions = 5000;
//        }
//
//        // 设置ES属性信息
//        System.setProperty("es.set.netty.runtime.available.processors", "false");
//        Map<String, String> config = new HashMap<String, String>();
//        config.put("cluster.name", clusterName);
//        config.put("client.transport.sniff", "true");
//        config.put("transport.type", "netty3");
//        config.put("http.type", "netty3");
////        config.put("bulk.flush.backoff.enable", "true");
////        config.put("bulk.flush.backoff.type", "CONSTANT");
////        config.put("bulk.flush.backoff.delay", "2");
////        config.put("bulk.flush.backoff.retries", "3");
//
//        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(maxActions));
//        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, "20"); //mb
//        config.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, "15"); //毫秒
//
//
//        // 设置ES集群节点列表
//        List<InetSocketAddress> transportAddresses = new ArrayList<>();
//        ///transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.95.114"), 58940));
//        ///transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.95.117"), 58940));
//        if(null!=esTransPorts && esTransPorts.trim().length()>0){
//            for (String s : esTransPorts.split(",")) {
//                String[] transPort = s.split(":");
//                transportAddresses.add(new InetSocketAddress(transPort[0], Integer.parseInt(transPort[1])));
//            }
//        }
//
//
//        ElasticsearchSink<BizData> elasticsearchSink = null;
//        try{
//            // 创建ElasticsearchSinkFunction，需覆写process方法
//            ElasticsearchSinkFunction<BizData> elasticsearchSinkFunction = new ElasticsearchSinkFunction<BizData>() {
//                public IndexRequest createIndexRequest(BizData data) {
//
//                    int randx = ThreadLocalRandom.current().nextInt(20000000);
//                    long nextId = idGen.nextId();
//                    String indexId = "pm_" + "caijp_" + data.getKbpNo() + "_" + data.getKpiNo() + "_" + data.getDcTime() +"_"
//                            + randx + "_" + nextId + "_"+System.nanoTime();
//
//                    Map<String, String> map = new HashMap<>();
//                    map.put("KBP", data.getKbpNo() + "");
//                    map.put("KPI_NO", data.getKpiNo() + "");
//                    map.put("DCTIME", data.getDcTime() + "");
//                    map.put("VALUE", ConverUtils.Obj2Double(data.getStringValue())+"");
//                    map.put("GROUPID", "caijinpeng");
//                    map.put("WRITETIME", System.currentTimeMillis() +"");
//                    map.put("DRUINGTIME", String.valueOf((System.currentTimeMillis()- data.getReceiveTime())));
//                    map.put("DRUINGTIME1", String.valueOf((System.currentTimeMillis()- data.getDcTime())));
//
//                    loggr.info(">>>>>>> save es , indexId="+indexId+", KBP="+data.getKbpNo() +",  KPINO="+ data.getKpiNo()+", DCTIME="+data.getDcTime() );
//
//                    return Requests.indexRequest()
//                            .index(INDEX)
//                            .type(INDEX_TYPE).id(indexId).source(map);
//                }
//
//                @Override
//                public void process(BizData element, RuntimeContext ctx, RequestIndexer indexer) {
//                    indexer.add(createIndexRequest(element));
//                }
//            };
//
//
//            ActionRequestFailureHandler failureHandler = (ActionRequestFailureHandler) (action, failure, restStatusCode, indexer) -> {
//
//                if (failure instanceof EsRejectedExecutionException) {
//                    // 将失败请求继续加入队列，后续进行重试写入
//                    indexer.add(action);
//                } else if (failure instanceof ElasticsearchParseException) {
//                    // 添加自定义的处理逻辑
//                } else {
//                    ///throw failure;
//                }
//            };
//
//            // 根据conf、addresses、sinkFunction构建ElasticsearchSink实例
//            elasticsearchSink = new ElasticsearchSink<BizData>(config, transportAddresses, elasticsearchSinkFunction, failureHandler);
//        }catch(Exception ex){
//            loggr.error("ElasticsearchSink error! ", ex);
//        }
//
//        return elasticsearchSink;
//    }


}
