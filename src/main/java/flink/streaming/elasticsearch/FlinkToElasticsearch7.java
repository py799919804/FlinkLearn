package flink.streaming.elasticsearch;

//import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;


/**
 * @author caijinpeng
 * @Titile
 * @Description
 * @date 2020/2/7 16:57
 */
public class FlinkToElasticsearch7 {

//    private static final Logger loggr = LoggerFactory.getLogger(FlinkToElasticsearch7.class);
//
//    private static final String INDEX = "pm_raw_p_flink_200200"; // index name
//    private static final String INDEX_TYPE = "es7"; // document type
//
//    private static final IdGenerator idGen = IdGenerator.getInstance();
//
//    public static ElasticsearchSink<BizData> getElasticsearchSink(String esTransPorts, String clusterName, int maxActions) {
//        if(maxActions < 5000){
//            maxActions = 5000;
//        }
//
//
//
//        // 设置ES集群节点列表
//        List<HttpHost> transportAddresses = new ArrayList<>();
//        ///transportAddresses.add(new HttpHost("192.168.95.114", 59300, "http"));
//        ///transportAddresses.add(new InetSocketAddress(InetAddress.getByName("192.168.95.117"), 58940));
//        if(null!=esTransPorts && esTransPorts.trim().length()>0){
//            for (String s : esTransPorts.split(",")) {
//                String[] transPort = s.split(":");
//                transportAddresses.add(new HttpHost(transPort[0], Integer.parseInt(transPort[1]), "http"));
//            }
//        }
//
//
//        ElasticsearchSink.Builder<BizData> elasticsearchSink = null;
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
//            elasticsearchSink = new ElasticsearchSink.Builder<BizData>(transportAddresses, elasticsearchSinkFunction);
//            elasticsearchSink.setBulkFlushMaxActions(10000);
//            elasticsearchSink.setBulkFlushMaxSizeMb(15);
//            elasticsearchSink.setBulkFlushInterval(15);
//        }catch(Exception ex){
//            loggr.error("ElasticsearchSink error! ", ex);
//        }
//
//        return elasticsearchSink.build();
//    }


}
