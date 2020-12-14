package elasticsearch;

import com.ultrapower.fsms.common.zookeeper.ZookeeperClient;
import com.ultrapower.fsms.common.zookeeper.ZookeeperConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ESRestUtils {
    private static RestHighLevelClient client = null;
    private static BulkProcessor bulkProcessor = null;
    // 用来定期刷新Bulk数据的线程
    private static Thread deamon;

    static {
        synchronized (ESRestUtils.class) {
            if (client == null) {
                initClient();
                initBulkProcessor();
            }
        }
    }

    public static void initClient() {
        String esUserName = System.getProperty(ZookeeperConstant.LocalZKProperty.LOCAL_ES_URL);
        String esPassword = System.getProperty(ZookeeperConstant.LocalZKProperty.LOCAL_ES_PASSWORD);
        String esUrl = System.getProperty(ZookeeperConstant.LocalZKProperty.LOCAL_ES_URL);
        String esClusterName = System.getProperty(ZookeeperConstant.LocalZKProperty.LOCAL_ES_CLUSTERNAME);
        try{
            if(null==esUserName || StringUtils.isBlank(esUserName)){
                esUserName = ZookeeperClient.getInstance().getData(ZookeeperConstant.key_es_user);
            }
            if(null==esPassword || StringUtils.isBlank(esPassword)){
                esPassword = ZookeeperClient.getInstance().getData(ZookeeperConstant.key_es_pwd);
            }
            if(null==esUrl || StringUtils.isBlank(esUrl)){
                esUrl = ZookeeperClient.getInstance().getData(ZookeeperConstant.key_es_url);
            }
            if(null==esClusterName || StringUtils.isBlank(esClusterName)){
                esClusterName = ZookeeperClient.getInstance().getData(ZookeeperConstant.key_es_clustername);
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }


        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(esUserName, esPassword));


        String[] clusters = esUrl.toString().split(",");
        HttpHost[] nodes = new HttpHost[clusters.length];
        for (int i = 0; i < clusters.length; i++) {
            String[] tmp = clusters[i].split(":");
            nodes[i] = new HttpHost(tmp[0], Integer.parseInt(tmp[1]), "http");
        }

        client = new RestHighLevelClient(RestClient.builder(nodes).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        }).setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestBuilder) {
                requestBuilder.setConnectTimeout(5000);
                requestBuilder.setSocketTimeout(40000);
                requestBuilder.setConnectionRequestTimeout(1000);
                return requestBuilder;
            }
        }));

        System.out.println("==================init elasticsearch client = " + client);

        // 关闭client
        // client.close();
    }

    public static void initBulkProcessor() {
        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) -> {
                    //client.bulk(request, RequestOptions.DEFAULT);
                    client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
                },
                new BulkProcessor.Listener() {
                    @Override
                    public void beforeBulk(long executionId, BulkRequest request) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    }

                    @Override
                    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    }
                });
        builder.setBulkActions(10000);
        builder.setBulkSize(new ByteSizeValue(50L, ByteSizeUnit.MB));
        builder.setConcurrentRequests(16);
        //builder.setFlushInterval(TimeValue.timeValueSeconds(5L));
        builder.setBackoffPolicy(BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(1000), 3));

        bulkProcessor = builder.build();
        deamon = new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("============init bulkProcessor==============");
                while (true) {
                    try {
                        //不使用BulkProcessor内置的刷新机制，改为定时刷新
                        // 目的是防止数据没有达到刷新时间点，程序就已经运行完毕了。。。
                        TimeUnit.SECONDS.sleep(5);
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

    public void closeAll() {
        if (null != deamon) {
            deamon.stop();
        }
        if (null != bulkProcessor) {
            //关闭连接之前刷新数据
            bulkProcessor.flush();
            bulkProcessor.close();
        }
        if (null != client) {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

 /*   @SuppressWarnings("deprecation")
    public  boolean createIndex(String indexName, boolean deleteIfExist) {
        boolean needCreate = false;
        try {
            if (client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
                if (deleteIfExist) {
                    client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
                    needCreate = true;
                }
            } else {
                needCreate = true;
            }
            if (needCreate) {
                CreateIndexRequest request = new CreateIndexRequest(indexName);
                request.settings(Settings.builder().put("index.number_of_shards", 10).put("index.number_of_replicas", 2));
                CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
                needCreate = response.isAcknowledged();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return needCreate;
    }*/

    public  boolean createIndex(String indexName, int numberOfShards, int numberOfReplicas) {
        boolean needCreate = false;
        try {
            if (client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
                 throw new Exception("索引：" + indexName + " 已存在，请检查！");
            } else {
                needCreate = true;
            }
            if (needCreate) {
                CreateIndexRequest request = new CreateIndexRequest(indexName);
                request.settings(Settings.builder().put("index.number_of_shards", numberOfShards).put("index.number_of_replicas", numberOfReplicas));
                CreateIndexResponse response = client.indices().create(request, RequestOptions.DEFAULT);
                needCreate = response.isAcknowledged();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return needCreate;
    }

    public  boolean indexExist(String indexName) {
        boolean exist = false;
        try {
            exist = client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return exist;
    }

    public  String add(String indexName, String id, Map<String, Object> map) {
        Map<String, Object> properties = new HashMap<>();
        properties.putAll(map);
        try {
            if (StringUtils.isEmpty(id)) {
                throw new Exception("===========待插入到ES中的ID为空！");
            }
            IndexRequest indexRequest = new IndexRequest(indexName).id(id).source(map);
            IndexResponse response = client.index(indexRequest, RequestOptions.DEFAULT);
            return response.getId();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public  void bulkAdd(String indexName, String id, Map<String, Object> map) {
        try {
            IndexRequest request = new IndexRequest(indexName).id(id).source(map);
            bulkProcessor.add(request);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public  Map<String, Object> getById(String indexName, String typeName, String id) {
        try {
            GetRequest getRequest = new GetRequest(indexName, id);
            //搜索数据
            GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);
            //输出结果
            //System.out.println(response.getSource());
            //关闭client
            //client.close();
            return response.getSource();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public  int deleteById(String indexName, String typeName, String id) {
        DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, id);
        DeleteResponse response = null;
        try {
            response = client.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response.status().getStatus();
    }

    public  int updateById(String indexName, String typeName, String id, Map<String, Object> map) {
        Map<String, Object> properties = new HashMap<>();
        properties.putAll(map);
        UpdateResponse response = null;
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
            for (String key : properties.keySet()) {
                builder.field(key, properties.get(key));
            }
            builder.endObject();
            //response = client.prepareUpdate(indexName, typeName, id).setDoc(builder).get();
            UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id).doc(map);
            response = client.update(updateRequest, RequestOptions.DEFAULT);
            return response.status().getStatus();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public  boolean dropIndex(String indexName) {
        boolean result = false;
        try {
            GetIndexRequest request = new GetIndexRequest(indexName);
            if (!client.indices().exists(request, RequestOptions.DEFAULT)) {
                throw new Exception("===================待删除的索引不存在！！！");
            }

            AcknowledgedResponse dResponse = client.indices().delete(new DeleteIndexRequest(indexName), RequestOptions.DEFAULT);
            result = dResponse.isAcknowledged();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (result) {
            System.out.println("==============删除索引：" + indexName + " 成功！");
        }
        return result;
    }

    //   "inline": "ctx._source['extra'] = 'test'"
    public  long updateByQuery(String index, QueryBuilder query, String scriptStr, Map<String, Object> params) {
        UpdateByQueryRequest ubqrb = new UpdateByQueryRequest(index);
        Script script = new Script(ScriptType.INLINE,
                Script.DEFAULT_SCRIPT_LANG,
                scriptStr,
                params);

        ubqrb
                .setScript(script)
                .setQuery(query)
                .setAbortOnVersionConflict(false);

        long updatedCount = 0L;
        try {
            updatedCount = client.updateByQuery(ubqrb, RequestOptions.DEFAULT).getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return updatedCount;
    }

    public  String upSertById(String indexName, String typeName, String id, Map<String, Object> map) {
        try {
            // 设置查询条件, 查找不到则添加生效
            IndexRequest indexRequest = new IndexRequest(indexName, typeName, id).source(map);
            UpdateRequest upSet = new UpdateRequest(indexName, typeName, id)
                    .doc(map)
                    .upsert(indexRequest);

            return client.update(upSet, RequestOptions.DEFAULT).getId();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public SearchResponse search(String indexName, String typeName, QueryBuilder builder, Map<String, AbstractAggregationBuilder> aggregations, List<String> fields, Map<String, SortOrder> sortMap, int from, int size) {

        SearchRequest searchRequest = new SearchRequest(indexName).types(typeName);
        SearchSourceBuilder requstBuider = new SearchSourceBuilder();

        requstBuider.from(from).size(size);
        requstBuider.query(builder);

        if (null != sortMap && 0 != sortMap.size()) {
            for (String field : sortMap.keySet()) {
                requstBuider.sort(field, sortMap.get(field));
            }
        }
        if (null != fields && fields.size() > 0) {
            requstBuider.storedFields(fields);
        }

        if ((aggregations != null) && (aggregations.size() > 0))
            for (Iterator localIterator = aggregations.keySet().iterator(); localIterator.hasNext(); ) {
                String key = (String) localIterator.next();
                requstBuider.aggregation((AbstractAggregationBuilder) aggregations.get(key));
            }

        //System.out.println("ES Request : " + requstBuider.toString() + "\n");
        try {
            return client.search(searchRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static void main(String[] args) {
        //createIndex("business3", true);
        //dropIndex("mytest1");

		/*Map<String,Object> map = new HashMap<String, Object>();
    	long startTime = System.currentTimeMillis();
    	int count = 10000;
    	String msg = "{\"timeStamp\":\"1566869618578\",\"collectIp\":\"172.30.204.57\",\"ip\":\"10.4.160.110\",\"host\":\"10.4.160.110\",\"fileName\":\"cboss_web9.log\",\"collectTime\":\"1566869618578\",\"spendTime\":\"5175\",\"uuid\":\"beabb0e1c007480e9507761f38c9bac8\",\"crmLog\":\"CbossWeb下行\",\"instanceName\":\"cboss_web_9\",\"splitflag\":\"1-1\",\"msg\":\"2019-08-27 09:35:37,628 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.spring.Home - Start of processing of 'BIP1A172_T1000167_99708080-upss3-rrlmr20190827093537547737387_102019082709353753039206766811_1566869737628_503418186'||2019-08-27 09:35:37,629 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.system.aspect.HomeRequestBomcMonitorAspect - #BOMC业务端到端充值开机|CbossWeb下行|10999201908270935375365652166886|13552459169|20190827093537628|20190827093537629|1ms|成功|18090||httpWorkerThread-18090-8_177136|10.4.160.110||10999201908270935375365652166886||2019-08-27 09:35:37,631 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.system.aspect.HomeRequestBizLogAsyncAspect - 请求报文: <?xml version='1.0' encoding='UTF-8'?><InterBOSS><Version>0100<\\/Version><TestFlag>0<\\/TestFlag><BIPType><BIPCode>BIP1A172<\\/BIPCode><ActivityCode>T1000167<\\/ActivityCode><ActionCode>0<\\/ActionCode><\\/BIPType><RoutingInfo><OrigDomain>UPSS<\\/OrigDomain><RouteType>01<\\/RouteType><Routing><HomeDomain>BOSS<\\/HomeDomain><RouteValue>13552459169<\\/RouteValue><\\/Routing><\\/RoutingInfo><TransInfo><SessionID>102019082709353753039206766811<\\/SessionID><TransIDO>102019082709353753039206766811<\\/TransIDO><TransIDOTime>20190827093537<\\/TransIDOTime><\\/TransInfo><SNReserve><TransIDC>99708080-upss3-rrlmr20190827093537547737387<\\/TransIDC><ConvID>4eb6d86d-6ba9-4511-a161-cbf42d645cd6<\\/ConvID><CutOffDay>20190827<\\/CutOffDay><OSNTime>20190827093537<\\/OSNTime><OSNDUNS>9970<\\/OSNDUNS><HSNDUNS>1000<\\/HSNDUNS><MsgSender>0233<\\/MsgSender><MsgReceiver>1001<\\/MsgReceiver><Priority>3<\\/Priority><ServiceLevel>5<\\/ServiceLevel><\\/SNReserve><\\/InterBOSS>||2019-08-27 09:35:37,636 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.ResponseActFactory - CbossResponseConfigDataModule=[BIPCODE=BIP1A172;ACTIVITY_CODE=T1000167;RESPONSE_CLASS=com.asiainfo.cboss.web.home.template.n2n.N2NHomeTemplateResp;NOTES=充值接口（全网方案）;EXT1=CENTER;EXT2=null;EXT3=null;]||2019-08-27 09:35:37,637 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.ResponseActFactory - 本次请求的处理类为 com.asiainfo.cboss.web.home.template.n2n.N2NHomeTemplateResp@3c80d4a3||2019-08-27 09:35:37,628 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.spring.Home - Start of processing of 'BIP1A172_T1000167_99708080-upss3-rrlmr20190827093537547737387_102019082709353753039206766811_1566869737628_503418186'||2019-08-27 09:35:37,629 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.system.aspect.HomeRequestBomcMonitorAspect - #BOMC业务端到端充值开机|CbossWeb下行|10999201908270935375365652166886|13552459169|20190827093537628|20190827093537629|1ms|成功|18090||httpWorkerThread-18090-8_177136|10.4.160.110||10999201908270935375365652166886||2019-08-27 09:35:37,631 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.system.aspect.HomeRequestBizLogAsyncAspect - 请求报文: <?xml version='1.0' encoding='UTF-8'?><InterBOSS><Version>0100<\\/Version><TestFlag>0<\\/TestFlag><BIPType><BIPCode>BIP1A172<\\/BIPCode><ActivityCode>T1000167<\\/ActivityCode><ActionCode>0<\\/ActionCode><\\/BIPType><RoutingInfo><OrigDomain>UPSS<\\/OrigDomain><RouteType>01<\\/RouteType><Routing><HomeDomain>BOSS<\\/HomeDomain><RouteValue>13552459169<\\/RouteValue><\\/Routing><\\/RoutingInfo><TransInfo><SessionID>102019082709353753039206766811<\\/SessionID><TransIDO>102019082709353753039206766811<\\/TransIDO><TransIDOTime>20190827093537<\\/TransIDOTime><\\/TransInfo><SNReserve><TransIDC>99708080-upss3-rrlmr20190827093537547737387<\\/TransIDC><ConvID>4eb6d86d-6ba9-4511-a161-cbf42d645cd6<\\/ConvID><CutOffDay>20190827<\\/CutOffDay><OSNTime>20190827093537<\\/OSNTime><OSNDUNS>9970<\\/OSNDUNS><HSNDUNS>1000<\\/HSNDUNS><MsgSender>0233<\\/MsgSender><MsgReceiver>1001<\\/MsgReceiver><Priority>3<\\/Priority><ServiceLevel>5<\\/ServiceLevel><\\/SNReserve><\\/InterBOSS>||2019-08-27 09:35:37,636 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.ResponseActFactory - CbossResponseConfigDataModule=[BIPCODE=BIP1A172;ACTIVITY_CODE=T1000167;RESPONSE_CLASS=com.asiainfo.cboss.web.home.template.n2n.N2NHomeTemplateResp;NOTES=充值接口（全网方案）;EXT1=CENTER;EXT2=null;EXT3=null;]||2019-08-27 09:35:37,637 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.ResponseActFactory - 本次请求的处理类为 com.asiainfo.cboss.web.home.template.n2n.N2NHomeTemplateResp@3c80d4a3||2019-08-27 09:35:37,640 [httpWorkerThread-18090-8] ERROR com.asiainfo.cboss.web.home.template.zj.HomeTemplateResp - 校验请求报文: <?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?><PaymentReq><IDType>01<\\/IDType><IDValue>13552459169<\\/IDValue><TransactionID>10999201908270935375365652166886<\\/TransactionID><BusiTransID>ac5f102cab5648749d808d47ce699281<\\/BusiTransID><ActionDate>20190827<\\/ActionDate><ActionTime>20190827093537<\\/ActionTime><ChargeMoney>1000<\\/ChargeMoney><OrganID>0051<\\/OrganID><CnlTyp>81<\\/CnlTyp><PayedType>01<\\/PayedType><SettleDate>20190827<\\/SettleDate><OrderNo>7699b7caf62842efb1717f71f9602904<\\/OrderNo><ProductNo>YWSAPQGNW<\\/ProductNo><Payment>999<\\/Payment><OrderCnt>1<\\/OrderCnt><Commision>3<\\/Commision><RebateFee>0<\\/RebateFee><CreditCardFee>0<\\/CreditCardFee><ServiceFee>1<\\/ServiceFee><ActivityNo>99990020010810<\\/ActivityNo><\\/PaymentReq>\"}";
    	for (int i = 0; i < count; i++) {
    		map.put("name", msg);
    		map.put("taskId", "" + i);
    		map.put("templateId", "1");
    		map.put("status", "1");
    		map.put("状态", "1");
    		map.put("createTime", new Date());
    		//bulkAdd("mytest1", "test", map);
    		//add("mytest1", "test", i + "" , map);
    		bulkAdd("mytest1", "test", i + "", map);
    		//upSertById("student", "student", "" + i, map);
		}
    	long endTime = System.currentTimeMillis();
    	System.out.println("插入 " + count + " 条数据耗时：" + (endTime  - startTime)  + " ms");*/

		/*
		QueryBuilder query =new BoolQueryBuilder().must(QueryBuilders.termQuery("taskId", "2"));
		Map<String,Object> params = new HashMap<String, Object>();
		params.put("status", 4);
		updateByQuery("mytest1", "test", query, "ctx._source['status']=params.status", params);
		*/
        //closeAll();
    }
}
