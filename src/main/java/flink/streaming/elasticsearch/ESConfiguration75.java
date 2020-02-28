package flink.streaming.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Map;

/**
 * @author caijinpeng
 * @Titile
 * @Description
 * @date 2020/2/12 14:11
 */
public class ESConfiguration75 {
    public static final Logger logger = LoggerFactory.getLogger(ESConfiguration75.class);

    //Java Low Level REST Client （要想使用高版本client必须依赖低版本的client）
    private static RestClient client;
    //Java High Level REST Client （高版本client）
    public static RestHighLevelClient restHighLevelClient = null;

    public BulkProcessor bulkProcessor;
    public BulkProcessor.Listener listener;

    //用来定期刷新Bulk数据的线程
    private static Thread deamon;

    private String esTransPorts;

    private String clusterName;

    private int maxActions;


    public ESConfiguration75(String esTransPorts, String clusterName, int maxActions){
        if(maxActions < 5000){
            maxActions = 5000;
        }
        this.esTransPorts = esTransPorts;
        this.clusterName = clusterName;
        this.maxActions = maxActions;

        if (null == client) {
            synchronized(ESConfiguration75.class) {
                initClient();
            }
        }
        initBulkProcessor();
    }


    private void initClient() {
        try{
            //System.setProperty("es.set.netty.runtime.available.processors", "false");

            RestClientBuilder builder = RestClient.builder(
                    new HttpHost(InetAddress.getByName("192.168.95.114"), Integer.parseInt("59300"),"http"));

            // 异步httpclient连接延时配置
            builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
                @Override
                public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                    requestConfigBuilder.setConnectTimeout(1000);
                    requestConfigBuilder.setSocketTimeout(3000);
                    requestConfigBuilder.setConnectionRequestTimeout(500);
                    return requestConfigBuilder;
                }
            });
            // 异步httpclient连接数配置
            builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                @Override
                public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                    httpClientBuilder.setMaxConnTotal(200);
                    httpClientBuilder.setMaxConnPerRoute(200);
                    return httpClientBuilder;
                }
            });
            client = builder.build();
            restHighLevelClient = new RestHighLevelClient(builder);
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }



    public void initBulkProcessor(){
        Settings settings = Settings.builder().put("node.name", "es7").build();
        ThreadPool threadPool = new ThreadPool(settings);
        listener = new BulkProcessor.Listener() {
            //bulk提交之前
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                //LOGGER.info("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            //bulk提交以后
            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    //LOGGER.error("Bulk [{}] executed with failures,response = {}", executionId, response.buildFailureMessage());
                } else {
                    //LOGGER.info("Bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
                }
            }
            //bulk提交以后并且返回异常
            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                //LOGGER.error("Failed to execute bulk", failure);
            }
        };


//
//        BulkProcessor bulkProcessor = BulkProcessor.builder(
//                (BiConsumer<BulkRequest, ActionListener<BulkResponse>>) client, listener)
//                .setBulkActions(10000)
//                .setBulkSize(new ByteSizeValue(15, ByteSizeUnit.MB))
//                .setFlushInterval(TimeValue.timeValueSeconds(5))
//                .setConcurrentRequests(50)
//                .setBackoffPolicy(
//                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), 3))
//                .build();
//
//
//        this.bulkProcessor = bulkProcessor;
    }

    ActionListener<BulkResponse> listener2 = new ActionListener<BulkResponse>(){

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {

        }

        @Override
        public void onFailure(Exception e) {

        }
    };

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

            BulkRequest bulkRequest = new BulkRequest();
            bulkRequest.add( new UpdateRequest(indexName,id).doc(map).upsert(map));

            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, listener2);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
