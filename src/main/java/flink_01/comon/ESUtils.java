package flink_01.comon;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ResourceBundle;

public class ESUtils  {
    //日志
    private static final Logger logger = LoggerFactory.getLogger(ESUtils.class);
    //要加载的配置文件
    private static final String CONFIG_NAME = "config";
    // 读取本地文件
    private static final ResourceBundle configResource = ResourceBundle.getBundle(CONFIG_NAME);

    private static String ESUrl ;
    private static RestHighLevelClient client;


    static {
        try {
            ESUrl = configResource.getString("es.http_url");

            String[] httpHost = ESUrl.split(",");
            String[] hostPort1 = httpHost[0].split(":");
            String[] hostPort2 = httpHost[1].split(":");
            String[] hostPort3 = httpHost[2].split(":");

            client = new RestHighLevelClient(RestClient.builder(
                            new HttpHost(hostPort1[0], Integer.parseInt(hostPort1[1])),
                            new HttpHost(hostPort2[0], Integer.parseInt(hostPort2[1])),
                            new HttpHost(hostPort3[0], Integer.parseInt(hostPort3[1]))));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //判断索引是否存在
    public static boolean indexExists(String index){
        boolean exists = false;
        GetIndexRequest request = new GetIndexRequest(index);
        //是返回本地信息还是从主节点检索状态
        request.local(false);
        request.humanReadable(true);
        try {
            exists = client.indices().exists(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exists;
    }

    //创建索引
    public static void createIndex(String index,int shards,int replicas){
        try {
            boolean exists = indexExists(index);
            if (!exists == true){
                CreateIndexRequest request = new CreateIndexRequest(index);
                request.settings(Settings.builder()
                        .put("index.number_of_shards", shards)
                        .put("index.number_of_replicas", replicas)
                        //指定mapping
                );

                CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
                if(createIndexResponse.isAcknowledged()){
                    logger.info("create success! (Indicates whether all of the nodes have acknowledged the request)");
                }else{
                    logger.warn("create success! (Indicates some of the nodes have acknowledged the request)");
                };
                if(createIndexResponse.isShardsAcknowledged()){
                    logger.info("create success! (ndicates whether the requisite number of shard copies were started for each shard in the index before timing out)");
                }else{
                    logger.warn("create success! (ndicates some of shard copies were started for each shard in the index before timing out)");
                };

            }else{
                logger.warn("create failed! (index existed)");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //删除索引
    public static void deleteIndex(String index){
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
            if(deleteIndexResponse.isAcknowledged()){
                logger.info("delete sucess! (Indicates whether all of the nodes have acknowledged the request)");
            }else{
                logger.warn("delete sucess! (Indicates some of the nodes have acknowledged the request)");
            };
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                logger.warn("delete faild!  (index is not existed!)");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    //查询索引
    public static void queryIndexInRange(String index,int start,int end){
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.from(start);
        searchSourceBuilder.size(end);
        searchSourceBuilder.query(QueryBuilders.termQuery("GROUPID", "caijinpeng"));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = searchResponse.getHits();
            final SearchHit[] hits1 = hits.getHits();
            for(SearchHit hit: hits1){
                System.out.println(hit);
            }
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




    public static void main(String[] args) throws IOException {
        System.out.println(indexExists("pytest"));
//        createIndex("pytest",3,0);
//        deleteIndex("pytest");
    }


//    new RestHighLevelClient() throws IOException;
}
