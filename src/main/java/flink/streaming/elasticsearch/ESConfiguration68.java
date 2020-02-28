package flink.streaming.elasticsearch;

/**
 * @author caijinpeng
 * @Titile
 * @Description
 * @date 2020/2/12 14:11
 */
public class ESConfiguration68 {
//    public static final Logger logger = LoggerFactory.getLogger(ESConfiguration73.class);
//
//
//    private static TransportClient client;
//
//    private static BulkProcessor bulkProcessor;
//
//    //用来定期刷新Bulk数据的线程
//    private static Thread deamon;
//
//    private String esTransPorts;
//
//    private String clusterName;
//
//    private int maxActions;
//
//
//    public ESConfiguration73(String esTransPorts, String clusterName, int maxActions){
//        if(maxActions < 5000){
//            maxActions = 5000;
//        }
//        this.esTransPorts = esTransPorts;
//        this.clusterName = clusterName;
//        this.maxActions = maxActions;
//
//        if (null == client) {
//            synchronized(ESConfiguration.class) {
//                initClient();
//            }
//        }
//        initBulkProcessor();
//    }
//
//
//    private void initClient() {
//        try{
//            System.setProperty("es.set.netty.runtime.available.processors", "false");
//            Settings settings = Settings.builder()
//                    .put("cluster.name", "es7")
//                    .put("client.transport.ping_timeout", "120s")
//                    .put("client.transport.sniff", true)
//                    .put("transport.type", "netty4")
//                    .put("http.type", "netty4")
//                    /*   .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE, true)
//                       .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE, true)
//                       .put("searchguard.ssl.transport.enabled", true)
//                       .put("searchguard.ssl.transport.keystore_filepath", CoreConfig.ES_KEYSTORE_LOCATION)
//                       .put("searchguard.ssl.transport.truststore_filepath", CoreConfig.ES_TRUSTSTORE_LOCATION)
//                       .put("searchguard.ssl.transport.keystore_password", CoreConfig.ES_KEYSTORE_PASS)
//                       .put("searchguard.ssl.transport.truststore_password", CoreConfig.ES_TRUSTSTORE_PASS)
//                       .put("searchguard.ssl.transport.enforce_hostname_verification", false)*/
//                    .build();
//            //创建client
//            //@SuppressWarnings("resource")
//            client = new PreBuiltTransportClient(settings);
//
//            //client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.95.114"), Integer.parseInt("58940")));
//            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.95.114"), Integer.parseInt("59300")));
//
//        }catch (Exception ex){
//            ex.printStackTrace();
//        }
//    }
//
//
//    private void initBulkProcessor() {
//        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
//            @Override
//            public void beforeBulk(long executionId, BulkRequest request) {
//            }
//
//            @Override
//            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
//            }
//
//            @Override
//            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
//            }
//        })
//        // 1w次请求执行一次bulk
//        // 1gb的数据刷新一次bulk
//        .setBulkActions(10000)
//        .setBulkSize(new ByteSizeValue(20, ByteSizeUnit.MB))
//        // 固定5s必须刷新一次
//        .setFlushInterval(TimeValue.timeValueSeconds(10))
//        // 并发请求数量, 0不并发, 1并发允许执行
//        .setConcurrentRequests(30)
//        // 设置退避, 100ms后执行, 最大请求3次
//        .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
//        .build();
//
//           deamon = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("============init bulkProcessor==============");
//                while (true) {
//                    try {
//                        //不使用BulkProcessor内置的刷新机制，改为定时刷新
//                        // 目的是防止数据没有达到刷新时间点，程序就已经运行完毕了。。。
//                        TimeUnit.SECONDS.sleep(1);
//                        bulkProcessor.flush();
//                        //System.out.println("============flush request to es ==============");
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        });
//        deamon.start();
//    }
//
//
//
//    /**
//     * TODO 批量插入数据，注意不会立即返回执行结果
//     * @method addDate
//     * @param indexName
//     * @param typeName
//     * @param id
//     * @param map
//     * @return
//     * @return String
//     * @date 2018年7月3日 下午4:42:40
//     */
//    public void bulkAdd(String indexName, String typeName, String id, Map<String,Object> map){
//        try {
//            IndexRequest request = new IndexRequest(indexName, typeName, id).source(map);
//            bulkProcessor.add(request);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

}
