package flink.streaming.elasticsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;


import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * @author caijinpeng
 * @Titile
 * @Description
 * @date 2020/2/12 14:11
 */
public class ESConfiguration76 {
    public static final Logger logger = LoggerFactory.getLogger(ESConfiguration76.class);

    private static TransportClient client;

    private static BulkProcessor bulkProcessor;

    //用来定期刷新Bulk数据的线程
    private static Thread deamon;

    private String esTransPorts;

    private String clusterName;

    private int maxActions;



    public ESConfiguration76(String esTransPorts, String clusterName, int maxActions){
        if(maxActions < 5000){
            maxActions = 5000;
        }
        this.esTransPorts = esTransPorts;
        this.clusterName = clusterName;
        this.maxActions = maxActions;

        if (null == client) {
            synchronized(ESConfiguration76.class) {
                initClient();
            }
        }
        initBulkProcessor();
    }


    private void initClient() {
        try{
            ///System.setProperty("es.set.netty.runtime.available.processors", "false");
            Settings settings = Settings.builder()
                    .put("cluster.name", this.clusterName)
                    .put("client.transport.ping_timeout", "120s")
                    .put("client.transport.sniff", true)
                    .put("transport.type", "netty4")
                    .put("http.type", "netty4")
                    /*   .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE, true)
                       .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE, true)
                       .put("searchguard.ssl.transport.enabled", true)
                       .put("searchguard.ssl.transport.keystore_filepath", CoreConfig.ES_KEYSTORE_LOCATION)
                       .put("searchguard.ssl.transport.truststore_filepath", CoreConfig.ES_TRUSTSTORE_LOCATION)
                       .put("searchguard.ssl.transport.keystore_password", CoreConfig.ES_KEYSTORE_PASS)
                       .put("searchguard.ssl.transport.truststore_password", CoreConfig.ES_TRUSTSTORE_PASS)
                       .put("searchguard.ssl.transport.enforce_hostname_verification", false)*/
                    .build();
            //创建client
            //@SuppressWarnings("resource")
            client = new PreBuiltTransportClient(settings);


//            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.236"), Integer.parseInt("59300")));
//            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.237"), Integer.parseInt("59300")));
//            client.addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.182.235"), Integer.parseInt("59300")));
           if(null!=esTransPorts && esTransPorts.trim().length()>0) {
               for (String s : esTransPorts.split(",")) {
                   String[] transPort = s.split(":");
                   client.addTransportAddress(new TransportAddress(InetAddress.getByName(transPort[0]), Integer.parseInt(transPort[1])));
               }
           }
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }


    private void initBulkProcessor() {
        bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            }
        })
                // 1w次请求执行一次bulk
                // 1gb的数据刷新一次bulk
                .setBulkActions(10000)
                .setBulkSize(new ByteSizeValue(20, ByteSizeUnit.MB))
                // 固定5s必须刷新一次
                .setFlushInterval(TimeValue.timeValueSeconds(10))
                // 并发请求数量, 0不并发, 1并发允许执行
                .setConcurrentRequests(25)
                // 设置退避, 100ms后执行, 最大请求3次
                .setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
                .build();

        deamon = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("============init bulkProcessor==============");
                while (true) {
                    try {
                        //不使用BulkProcessor内置的刷新机制，改为定时刷新
                        // 目的是防止数据没有达到刷新时间点，程序就已经运行完毕了。。。
                        TimeUnit.SECONDS.sleep(1);
                        bulkProcessor.flush();
                        //System.out.println("============flush request to es ==============");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        deamon.start();
    }



    /**
     * 批量插入数据，注意不会立即返回执行结果
     * @method bulkAdd
     * @param indexName
     * @param id
     * @param map
     * @return
     * @return
     */
    public void bulkAdd(String indexName, String id, Map<String,Object> map){
        try {
            UpdateRequest request = new UpdateRequest(indexName, id).doc(map).upsert(map);
            bulkProcessor.add(request);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
