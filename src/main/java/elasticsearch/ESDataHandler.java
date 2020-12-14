package elasticsearch;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @Title: ES数据操作类
 * @description:
 *
 * @Company: ultrapower.com
 * @author lnj2050
 * @create time：2016年5月13日 下午6:02:30
 * @version 1.0
 */
public class ESDataHandler {
    // 日志
    private static Logger log = LoggerFactory.getLogger(ESDataHandler.class);



    // 取得TransportClient对象
    public TransportClient getTransportClient() {
        return ESClientUtil.getInstance().getClient();
    }


    /**
     * 创建索引
     *
     * @param indexname 索引名
     * @param type 索引类型
     * @param indexId 索引id
     * @param map 索引源数据
     * @throws Exception
     */
    public void createIndex(String indexname, String type, String indexId, Map<String, Object> map) throws Exception {
        IndexResponse response = getTransportClient().prepareIndex(indexname, type, indexId).setSource(map).get();
        RestStatus restStatus = response.status();
        int status = restStatus.getStatus();
        if (status == 201) {
            log.debug("createIndex indexname=" + indexname + ",type=" + type + ",indexId=" + indexId + " created!");
        } else {
            log.debug("createIndex indexname=" + indexname + ",type=" + type + ",indexId=" + indexId + " no created!");
        }

    }

    /**
     * 创建索引
     *
     * @param indexname 索引名
     * @param type 索引类型
     * @param dataMap 数据 key=id(索引id)，Map=该id对应的数据
     * @throws Exception
     */
    public void createIndex(String indexname, String type, Map<String, Map<String, Object>> dataMap) throws Exception {
        if (dataMap == null || dataMap.size() <= 0) {
            log.error("createIndex dataMap is null");
            return;
        }

        Iterator<Map.Entry<String, Map<String, Object>>> it = dataMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Map<String, Object>> entry = it.next();
            String indexId = entry.getKey();
            Map<String, Object> map = entry.getValue();
            this.createIndex(indexname, type, indexId, map);
        }
    }


    /**
     * 批量创建索引
     *
     * @param indexname 索引名
     * @param type 索引类型
     * @param dataMap 数据 key=id(索引id)，Map=该id对应的数据
     * @throws Exception
     */
    public void createIndexBulk(String indexname, String type, Map<String, Map> dataMap) throws Exception {
        if (dataMap == null || dataMap.size() <= 0) {
            log.error("createIndexBulk dataMap is null");
            return;
        }

        List<IndexRequest> requests = new ArrayList<IndexRequest>();
        Iterator<Map.Entry<String, Map>> it = dataMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Map> entry = it.next();
            String indexId = entry.getKey();
            Map map = entry.getValue();
            IndexRequest request = getTransportClient().prepareIndex(indexname, type, indexId).setSource(map).request();
            requests.add(request);
        }

        BulkRequestBuilder bulkRequest = getTransportClient().prepareBulk();

        for (IndexRequest request : requests) {
            bulkRequest.add(request);
        }

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            log.error("createIndexBulk indexname="+indexname+",type="+indexname+" has failures, " + bulkResponse.buildFailureMessage());
        }
    }

    /**
     * 分批创建索引
     *
     * @param indexname 索引名
     * @param type 索引类型
     * @param dataMap 数据 key=id(索引id)，Map=该id对应的数据
     * @param commitNum 每commitNum条提交一次
     * @throws Exception
     */
    public void createIndexInBatch(String indexname, String type, Map<String, Map> dataMap, long commitNum) throws Exception {
        if (dataMap == null || dataMap.size() <= 0) {
            log.error("createIndexInBatch dataMap is null");
            return;
        }

        int count = 0;
        BulkRequestBuilder bulkRequest = getTransportClient().prepareBulk();
        Iterator<Map.Entry<String, Map>> it = dataMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Map> entry = it.next();
            String indexId = entry.getKey();
            Map map = entry.getValue();
            IndexRequest indexRequest = getTransportClient().prepareIndex(indexname, type, indexId).setSource(map).request();
            bulkRequest.add(indexRequest);
            count++;
            // 每一千条提交一次
            if (count % commitNum == 0) {
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();
                if (bulkResponse.hasFailures()) {
                    log.info(indexname + " commit fail, " + bulkResponse.buildFailureMessage());
                }

                bulkRequest.request().requests().clear();
                count = 0;
            }
        }

        if (count > 0) {
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();
            if (bulkResponse.hasFailures()) {
                log.info(indexname + " commit fail, " + bulkResponse.buildFailureMessage());
            }
        }
    }

    /**
     * 获取索引ID列表
     *
     * @param indexname
     * @param type
     * @param queryBuilder
     * @return
     */
    public List<String> getIndexIds(String indexname, String type, QueryBuilder queryBuilder) {
        List<String> list = new ArrayList<String>();

        SearchResponse searchResponse = null;
        try {
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexname).setTypes(type).setQuery(queryBuilder);
            searchResponse = srb.setScroll(new TimeValue(60000)).setSize(500).execute().actionGet();
        } catch (Exception e) {
            log.error("scroll query error,indexname=" + indexname + ", type=" + type + ", queryBuilder=" + queryBuilder.toString(), e);
        }

        if (searchResponse == null) {
            log.warn("searchResponse is null!!!!!!");
            return list;
        }

        SearchHits searchHit = searchResponse.getHits();
        long cursize = searchHit.getHits().length;
        if (cursize == 0) {
            return list;
        }
        for (SearchHit hit : searchHit) {
            list.add(hit.getId());
        }

        while (true) {
            searchResponse =
                    getTransportClient().prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();

            searchHit = searchResponse.getHits();
            cursize = searchHit.getHits().length;

            // 再次查询不到数据时跳出循环
            if (cursize == 0) {
                break;
            }
            for (SearchHit hit : searchHit) {
                list.add(hit.getId());
            }
        }

        return list;
    }


    /**
     * 批量删除索引
     *
     * @param indexname
     * @param type
     * @param ids
     */
    public void bulkRemoveESIndex(String indexname, String type, List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            log.info("resids is null");
            return;
        }
        BulkRequestBuilder builder = getTransportClient().prepareBulk();
        for (String id : ids) {
            builder.add(getTransportClient().prepareDelete(indexname, type, id).request());
        }
        BulkResponse bulkResponse = builder.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            log.warn("bulkRemoveESIndex fail, msg=" + bulkResponse.buildFailureMessage());
        }
    }

    /**
     * 删除一个索引
     * @param indexname
     * @param type
     * @param id
     */
    public void removeIndex(String indexname, String type, String id) {
        getTransportClient().prepareDelete(indexname, type, id).execute().actionGet();
    }

    /**
     * 刷新索引
     *
     * @param indexname
     */
    public void refreshIndex(String indexname) {
        getTransportClient().admin().indices().prepareRefresh(indexname).execute().actionGet();
    }


    /**
     * 根据条件， 获取所有查询结果列表
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @return
     * @author caijinpeng
     */
    public List<Map<String, Object>> getResultsByQueryBuilder(String indexName, String typeName, QueryBuilder queryBuilder) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();

        try {
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            if (queryBuilder != null) {
                srb = srb.setQuery(queryBuilder);
            }

            SearchResponse searchResponse = srb.setScroll(new TimeValue(60000)).setSize(500).execute().actionGet();
            SearchHits searchHit = searchResponse.getHits();
            long cursize = searchHit.getHits().length;
            if (cursize == 0) {
                return results;
            }
            for (SearchHit hit : searchHit) {
                Map<String, Object> map = hit.getSourceAsMap();
                if (map == null || map.isEmpty()) {
                    continue;
                }
                results.add(map);
            }

            while (true) {
                searchResponse = getTransportClient().prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();

                searchHit = searchResponse.getHits();
                cursize = searchHit.getHits().length;

                // 再次查询不到数据时跳出循环
                if (cursize == 0) {
                    break;
                }
                for (SearchHit hit : searchHit) {
                    Map<String, Object> map = hit.getSourceAsMap();
                    if (map == null || map.isEmpty()) {
                        continue;
                    }
                    results.add(map);
                }
            }
        }catch(Exception ex) {
            log.error("getAllResults error!! ", ex);
        }
        return results;
    }



    /**
     * 根据条件和排序条件， 获取所有查询结果列表(带排序)
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilder  排序条件
     * @return
     * @author caijinpeng
     */
    public List<Map<String, Object>> getResultsByQueryBuilder(String indexName, String typeName, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();

        try {
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            if (queryBuilder != null) {
                srb = srb.setQuery(queryBuilder);
            }
            if (sortBuilder != null) {
                srb = srb.addSort(sortBuilder);
            }

            SearchResponse searchResponse = srb.setScroll(new TimeValue(60000)).setSize(500).execute().actionGet();
            SearchHits searchHit = searchResponse.getHits();
            long cursize = searchHit.getHits().length;
            if (cursize == 0) {
                return results;
            }
            for (SearchHit hit : searchHit) {
                Map<String, Object> map = hit.getSourceAsMap();
                if (map == null || map.isEmpty()) {
                    continue;
                }
                results.add(map);
            }

            while (true) {
                searchResponse = getTransportClient().prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();

                searchHit = searchResponse.getHits();
                cursize = searchHit.getHits().length;

                // 再次查询不到数据时跳出循环
                if (cursize == 0) {
                    break;
                }
                for (SearchHit hit : searchHit) {
                    Map<String, Object> map = hit.getSourceAsMap();
                    if (map == null || map.isEmpty()) {
                        continue;
                    }
                    results.add(map);
                }
            }
        }catch(Exception ex) {
            log.error("getAllResults error!! ", ex);
        }
        return results;
    }



    /**
     * 根据条件和排序条件，  获取查询结果列表，分页展示  (带排序)(带分页)
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilder  排序条件
     * @param form
     * @param size
     * @return
     */
    public List<Map<String, Object>> getResultsByQueryBuilder(String indexName, String typeName, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, int form,
                                                              int size) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();

        try {
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            if (queryBuilder != null) {
                srb = srb.setQuery(queryBuilder);
            }
            if (sortBuilder != null) {
                srb = srb.addSort(sortBuilder);
            }
            if (form >= 10000) {
                int curpage = form / size;
                SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                for (int i = 0; i < curpage; i++) {
                    scrollResp =
                            getTransportClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    if (i == curpage - 1) {
                        for (SearchHit hit : scrollResp.getHits()) {
                            Map<String, Object> map = hit.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            results.add(map);
                        }
                    }
                }
            } else if (size >= 10000) {
                SearchResponse searchResponse = srb.setScroll(new TimeValue(60000)).setSize(500).execute().actionGet();
                SearchHits searchHit = searchResponse.getHits();
                long cursize = searchHit.getHits().length;
                if (cursize == 0) {
                    return results;
                }
                for (SearchHit hit : searchHit) {
                    Map<String, Object> map = hit.getSourceAsMap();
                    if (map == null || map.isEmpty()) {
                        continue;
                    }
                    results.add(map);
                }

                lop:while (true) {
                    searchResponse = getTransportClient().prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();

                    searchHit = searchResponse.getHits();
                    cursize = searchHit.getHits().length;

                    // 再次查询不到数据时跳出循环
                    if (cursize == 0) {
                        break;
                    }
                    for (SearchHit hit : searchHit) {
                        Map<String, Object> map = hit.getSourceAsMap();
                        if (map == null || map.isEmpty()) {
                            continue;
                        }
                        results.add(map);
                        if(results.size()==size){
                            break lop;
                        }
                    }
                }
            } else {
                SearchResponse searchResponse = srb.setFrom(form).setSize(size).execute().actionGet();
                if (searchResponse != null) {
                    SearchHits searchHits = searchResponse.getHits();
                    if (searchHits != null) {
                        for (SearchHit it : searchHits) {
                            Map<String, Object> map = it.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            results.add(map);
                        }
                    }
                }
            }

        }catch(Exception ex) {
            log.error("getResultsByQueryBuilderAndSortBuilder error!! ", ex);
        }
        return results;
    }


    /**
     * 根据条件和排序条件，  获取查询结果列表，分页展示  (带排序)(带分页)
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilder  排序条件
     * @param form
     * @param size
     * @return
     */
    public Map<String,Object> getResultsWithTotalByQueryBuilder(String indexName, String typeName, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, int form,
                                                                int size) {
        Map<String,Object> resultMap = new HashMap<>();
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        long total = 0;
        try {
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            if (queryBuilder != null) {
                srb = srb.setQuery(queryBuilder);
            }
            if (sortBuilder != null) {
                srb = srb.addSort(sortBuilder);
            }
            if (form >= 10000) {
                int curpage = form / size;
                SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                for (int i = 0; i < curpage; i++) {
                    scrollResp =
                            getTransportClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    if (i == curpage - 1) {
                        for (SearchHit hit : scrollResp.getHits()) {
                            Map<String, Object> map = hit.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }

                            results.add(map);
                        }

                    }
                }
                total = scrollResp.getHits().getTotalHits().value;
            } else if (size >= 10000) {
                SearchResponse searchResponse = srb.setScroll(new TimeValue(60000)).setSize(500).execute().actionGet();
                SearchHits searchHit = searchResponse.getHits();
                long cursize = searchHit.getHits().length;
                if (cursize == 0) {
                    return resultMap;
                }
                for (SearchHit hit : searchHit) {
                    Map<String, Object> map = hit.getSourceAsMap();
                    if (map == null || map.isEmpty()) {
                        continue;
                    }
                    results.add(map);
                }


                lop:while (true) {
                    searchResponse = getTransportClient().prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();

                    searchHit = searchResponse.getHits();
                    cursize = searchHit.getHits().length;

                    // 再次查询不到数据时跳出循环
                    if (cursize == 0) {
                        break;
                    }
                    for (SearchHit hit : searchHit) {
                        Map<String, Object> map = hit.getSourceAsMap();
                        if (map == null || map.isEmpty()) {
                            continue;
                        }
                        results.add(map);
                        if(results.size()==size){
                            break lop;
                        }
                    }
                }
            } else {
                SearchResponse searchResponse = srb.setFrom(form).setSize(size).execute().actionGet();
                if (searchResponse != null) {
                    SearchHits searchHits = searchResponse.getHits();
                    if (searchHits != null) {
                        for (SearchHit it : searchHits) {
                            Map<String, Object> map = it.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            results.add(map);
                        }
                        total = searchHits.getTotalHits().value;
                    }

                }
            }
            resultMap.put("count",total);
            resultMap.put("data",results);
        }catch(Exception ex) {
            log.error("getResultsByQueryBuilderAndSortBuilder error!! ", ex);
        }
        return resultMap;
    }

    /**
     * 根据条件，  获取查询结果列表，分页展示 (带分页)
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param form
     * @param size
     * @return
     */
    public List<Map<String, Object>> getResultsByQueryBuilder(String indexName, String typeName, QueryBuilder queryBuilder, int form, int size) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();

        try {
            if (queryBuilder == null) {
                log.warn("queryBuilder is null!!!");
                return results;
            }
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            srb = srb.setQuery(queryBuilder);
            //log.info(">>>>>>> queryBuilder >>>>>>>>>" + queryBuilder);
            results = this.getResultBySRB(srb, form, size);
        }catch(Exception ex) {
            log.error("getResultsByQueryBuilder error!! ", ex);
        }
        return results;
    }

    /**
     * 根据条件字符串，  获取查询结果列表，分页展示 (带分页)
     * @param indexName 索引名称
     * @param typeName  索引类型
     * @param queryStr  查询条件字符串
     * @param form
     * @param size
     * @return
     */
    public List<Map<String, Object>> getResultsByJsonstr(String indexName, String typeName, String queryStr, int form, int size) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            if (queryStr == null || queryStr.equals("")) {
                log.warn("queryStr is null!!!");
                return results;
            }
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            srb = srb.setQuery(getBoolQueryBuilder(queryStr));
            //log.info(">>>>>>> querystr >>>>>>>>>" + queryStr);
            results = this.getResultBySRB(srb, form, size);
        }catch(Exception ex) {
            log.error("getResultsByJsonstr error!! ", ex);
        }

        return results;
    }

    private List<Map<String, Object>> getResultBySRB(SearchRequestBuilder srb, int form, int size) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();

        try {
            if (form > 10000) {
                int curpage = form / size;
                SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                for (int i = 0; i < curpage; i++) {
                    scrollResp =
                            getTransportClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    if (i == curpage - 1) {
                        for (SearchHit hit : scrollResp.getHits()) {
                            Map<String, Object> map = hit.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            results.add(map);
                        }
                    }
                }
            } else if (size >= 10000) {
                SearchResponse searchResponse = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                if (searchResponse != null) {
                    SearchHits searchHits = searchResponse.getHits();
                    if (searchHits != null) {
                        for (SearchHit it : searchHits) {
                            Map<String, Object> map = it.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            results.add(map);
                        }
                    }
                }
            } else {
                SearchResponse searchResponse = srb.setFrom(form).setSize(size).execute().actionGet();
                if (searchResponse != null) {
                    SearchHits searchHits = searchResponse.getHits();
                    if (searchHits != null) {
                        for (SearchHit it : searchHits) {
                            Map<String, Object> map = it.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            results.add(map);
                        }
                    }
                }
            }
        }catch(Exception ex) {
            log.error("getResultBySRB error!! ", ex);
        }

        return results;
    }


    /**
     * 根据条件和排序条件， 获取查询的SearchHits
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilder  排序条件
     * @param form
     * @param size
     * @return
     */
    public SearchHits getSearchHits(String indexName, String typeName, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, int form, int size) {
        SearchHits searchHits = null;
        SearchRequestBuilder srb = null;
        try{
            srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            if (queryBuilder != null) {
                srb = srb.setQuery(queryBuilder);
            }

            if (form >= 10000) {
                int curpage = form / size;
                if (sortBuilder != null) {
                    srb = srb.addSort(sortBuilder);
                }
                SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                for (int i = 0; i < curpage; i++) {
                    scrollResp =
                            getTransportClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    if (i == curpage - 1) {
                        if (scrollResp != null) {
                            searchHits = scrollResp.getHits();
                        }
                    }
                }
            } else if (size >= 10000) {
                SearchResponse searchResponse = srb.setScroll(new TimeValue(60000)).setSize(10000).execute().actionGet();
                if (searchResponse != null) {
                    searchHits = searchResponse.getHits();
                }
            } else {
                if (sortBuilder != null) {
                    srb = srb.addSort(sortBuilder);
                }
                SearchResponse searchResponse = srb.setFrom(form).setSize(size).execute().actionGet();
                if (searchResponse != null) {
                    searchHits = searchResponse.getHits();
                }
            }

            //log.info(">>>>>>>> search condition >>>>>> " + srb.toString().replaceAll("\n", ""));
        }catch(Exception ex){
            log.error("getSearchHits error!!!  condition >>>>>> " + srb.toString().replaceAll("\n", ""), ex);
        }
        return searchHits;
    }

    /**
     * 获取多个字段的统计结果
     * @param indexName     索引名称
     * @param typeName      索引类型
     * @param queryBuilder  查询条件
     * @param fields        统计字段
     * @return
     */
    public Map<String, Map<String, Long>> getAggregations(String indexName, String typeName, QueryBuilder queryBuilder, String[] fields) {
        if (fields == null) {
            return null;
        }
        // 获取统计信息
        Map<String, Map<String, Long>> fieldsMap = new HashMap<String, Map<String, Long>>();
        try {
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            // 查询条件
            if (queryBuilder != null) {
                srb = srb.setQuery(queryBuilder);
            }

            // 设置统计字段
            for (String field : fields) {
                if (field == null || field.equals("")) {
                    continue;
                }
                srb.addAggregation(AggregationBuilders.terms(field).field(field).size(5000));
            }

            log.debug(">>>>>>> conditon >>>>>>>>>" + srb.toString().replaceAll("\n", ""));

            SearchResponse sr = srb.execute().actionGet();

            for (String field : fields) {
                if (field == null || field.equals("")) {
                    continue;
                }
                Map<String, Long> fieldMap = new HashMap<String, Long>();
                Terms terms = sr.getAggregations().get(field);
                for (Bucket b : terms.getBuckets()) {
                    fieldMap.put(String.valueOf(b.getKey()), b.getDocCount());
                }
                fieldsMap.put(field, fieldMap);
            }
        } catch (Exception ex) {
            log.error("getAggregations error", ex);
        }

        return fieldsMap;
    }




    /**
     * 判断索引是否在ES中存在
     *
     * @author liwei
     * @create time：2016-5-26  下午3:39:21
     * @param str_indexName  索引名称(pm_baseline_data)
     * @return true: 存在  false: 不存在
     */
    public boolean isExistInDBIndex(String str_indexName){
        boolean optFlag = false;
        try {
            optFlag = getTransportClient().admin().indices().prepareExists(str_indexName).get().isExists();
        } catch (Exception ex) {
            log.error("isExistInDBIndex(): exception! return false", ex);
            //close();
        } finally {
            //close();
        }

        return optFlag;
    }

    /**
     * 判断索引类型是否在ES中存在
     *
     * @author liwei
     * @create time：2016-5-26  下午3:39:21
     * @param str_indexName  索引名称(pm_baseline_data)
     * @param str_indexType  索引类型(flow-p-30, flow-c-21, flow-h-25)
     * @return true: 存在  false: 不存在
     */
    public boolean isExistInDBIndexType(String str_indexName, String str_indexType){
        boolean optFlag = false;
        try {
            boolean a = getTransportClient().admin().indices().prepareExists(str_indexName).get().isExists();
            if (!a) {
                //无索引时
                optFlag = false;
            } else {
                optFlag = getTransportClient().admin().indices().prepareTypesExists(str_indexName).setTypes(str_indexType).get().isExists();
            }
        } catch (Exception ex) {
            log.error("isExistInDBIndexType(): exception! return false", ex);
            //close();
        } finally {
            //close();
        }

        return optFlag;
    }



    /**
     * 判断索引中是否有数据
     *
     * @param indexName
     * @return
     */
    public boolean hasData(String indexName) {
        boolean result = false;
        long totalHits = 0;
        try {
            SearchRequestBuilder srb = ESClientUtil.getInstance().getClient().prepareSearch(indexName);
            SearchResponse searchResponse = srb.setFrom(0).setSize(1).execute().actionGet();
            if (searchResponse != null) {
                SearchHits searchHits = searchResponse.getHits();
                if (searchHits != null) {
                    totalHits = searchHits.getTotalHits().value;
                }
            }
        } catch (Exception e) {
            log.error("hasData error,indexName=" + indexName, e);
        }
        if (totalHits > 0) {
            result = true;
        }
        return result;
    }




    /**
     * 解析字符串查询条件
     *
     * @param queryStr
     * @return
     */
    public BoolQueryBuilder getBoolQueryBuilder(String queryStr) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        try {
            String sbstr = replaceJoiner(queryStr, " or ", " OR ");
            sbstr = replaceJoiner(sbstr, " and ", " AND ");
            sbstr = replaceJoiner(sbstr, " like ", " LIKE ");
            sbstr = replaceJoiner(sbstr, " notlike ", " NOTLIKE ");
            String[] shoudAry = sbstr.split(" OR ");
            int shoudlen = shoudAry.length;
            if (shoudlen == 1) {
                boolQueryBuilder = getMustQueryBuilder(shoudAry[0]);
            } else if (shoudlen > 1) {
                for (int i = 0; i < shoudlen; i++) {
                    String mustStr = shoudAry[i];
                    if (mustStr == null || mustStr.equals("")) {
                        continue;
                    }

                    boolQueryBuilder.should(getMustQueryBuilder(mustStr));
                }
            }
        }catch(Exception ex) {
            log.error("getBoolQueryBuilder error!!! ", ex);
        }
        return boolQueryBuilder;
    }


    private BoolQueryBuilder getMustQueryBuilder(String mustStr) {
        BoolQueryBuilder mustQueryBuilder = QueryBuilders.boolQuery();
        try {
            //log.info("getMustQueryBuilder request mustStr >>>>: "+ mustStr);
            String[] mustAry = mustStr.split(" AND ");
            for (int j = 0; j < mustAry.length; j++) {
                String condition = mustAry[j];
                if (condition == null || condition.equals("")) {
                    continue;
                }
                QueryBuilder queryBuilder = null;
                if (condition.indexOf("!=") > 0) {
                    String[] ocary = condition.split("!=");
                    queryBuilder = QueryBuilders.termQuery(ocary[0].trim(), ocary[1].trim());
                    mustQueryBuilder.mustNot(queryBuilder);
                } else if (condition.indexOf(" NOTLIKE ") > 0) {
                    String[] ocary = condition.split(" NOTLIKE ");
                    queryBuilder = QueryBuilders.wildcardQuery(ocary[0].trim(), ocary[1].trim());
                    mustQueryBuilder.mustNot(queryBuilder);
                } else {
                    if (condition.indexOf(" LIKE ") > 0) {
                        String[] ocary = condition.split(" LIKE ");
                        queryBuilder = QueryBuilders.wildcardQuery(ocary[0].trim(), ocary[1].trim());
                    } else if (condition.indexOf(" gt ") > 0 && (condition.indexOf(" lt ") < 0 || condition.indexOf(" lte ") < 0)) {
                        String[] ocary = condition.split(" gt ");
                        queryBuilder = QueryBuilders.rangeQuery(ocary[0].trim()).gt(ocary[1].trim());
                    } else if (condition.indexOf(" gte ") > 0 && (condition.indexOf(" lt ") < 0 || condition.indexOf(" lte ") < 0)) {
                        String[] ocary = condition.split(" gte ");
                        queryBuilder = QueryBuilders.rangeQuery(ocary[0].trim()).gte(ocary[1].trim());
                    } else if ((condition.indexOf(" gt ") < 0 || condition.indexOf(" gte ") < 0) && condition.indexOf(" lt ") > 0) {
                        String[] ocary = condition.split(" lt ");
                        queryBuilder = QueryBuilders.rangeQuery(ocary[0].trim()).lt(ocary[1].trim());
                    } else if ((condition.indexOf(" gt ") < 0 || condition.indexOf(" gte ") < 0) && condition.indexOf(" lte ") > 0) {
                        String[] ocary = condition.split(" lte ");
                        queryBuilder = QueryBuilders.rangeQuery(ocary[0].trim()).lte(ocary[1].trim());
                    } else if (condition.indexOf(" gt ") > 0 && condition.indexOf(" lt ") > 0) {
                        int gtidx = condition.indexOf(" gt ");
                        String field = condition.substring(0, gtidx);
                        String tmpcondition = condition.substring(gtidx + 3);
                        String[] ocary = tmpcondition.split(" lt ");
                        queryBuilder = QueryBuilders.rangeQuery(field).gt(ocary[0].trim()).lt(ocary[1].trim());
                    } else if (condition.indexOf(" gte ") > 0 && condition.indexOf(" lt ") > 0) {
                        int gtidx = condition.indexOf(" gte ");
                        String field = condition.substring(0, gtidx);
                        String tmpcondition = condition.substring(gtidx + 4);
                        String[] ocary = tmpcondition.split(" lt ");
                        queryBuilder = QueryBuilders.rangeQuery(field).gte(ocary[0].trim()).lt(ocary[1].trim());
                    } else if (condition.indexOf(" gt ") > 0 && condition.indexOf(" lte ") > 0) {
                        int gtidx = condition.indexOf(" gt ");
                        String field = condition.substring(0, gtidx);
                        String tmpcondition = condition.substring(gtidx + 3);
                        String[] ocary = tmpcondition.split(" lte ");
                        queryBuilder = QueryBuilders.rangeQuery(field).gt(ocary[0].trim()).lte(ocary[1].trim());
                    } else if (condition.indexOf(" gte ") > 0 && condition.indexOf(" lte ") > 0) {
                        int gtidx = condition.indexOf(" gte ");
                        String field = condition.substring(0, gtidx);
                        String tmpcondition = condition.substring(gtidx + 4);
                        String[] ocary = tmpcondition.split(" lte ");
                        queryBuilder = QueryBuilders.rangeQuery(field).gte(ocary[0].trim()).lte(ocary[1].trim());
                    } else if (condition.indexOf("=") > 0) {
                        String[] ocary = condition.split("=");
                        // 如果值本身包含逗号，这种情况有问题
                        //if (ocary[1].indexOf(",") > 0) {
                        //    Object[] valary = ocary[1].trim().split(",");
                        //    queryBuilder = QueryBuilders.termsQuery(ocary[0].trim(), valary);
                        //} else {
                        queryBuilder = QueryBuilders.termQuery(ocary[0].trim(), ocary[1].trim());
                        //}
                    }
                    if (queryBuilder != null) {
                        mustQueryBuilder.must(queryBuilder);
                    }
                }

            }
        }catch(Exception ex) {
            log.error("getMustQueryBuilder error!!! ", ex);
        }
        return mustQueryBuilder;
    }


    private String replaceJoiner(String input, String regex, String replacement) {
        Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(input);
        String result = m.replaceAll(replacement);
        return result;
    }

    /**
     * 批量更新某个字段的值
     * @param indexname 索引名称
     * @param typename 索引类型
     * @param ids 索引id集合
     * @param dataMap 更新的数据
     */
    public void bulkUpdateIndexData(String indexname, String typename, List<String> ids, Map<String, Object> dataMap) {
        if (dataMap == null || dataMap.isEmpty()) {
            return;
        }

        try {
            int count = 0;
            String idstr = "";
            BulkRequestBuilder builder = getTransportClient().prepareBulk();
            for (String id : ids) {
                UpdateRequestBuilder urb = null;
                try {
                    XContentBuilder mapping = XContentFactory.jsonBuilder().startObject();
                    Iterator it = dataMap.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, Object> me = (Map.Entry<String, Object>) it.next();
                        mapping.field(me.getKey(), me.getValue());
                    }
                    mapping.endObject();
                    urb = getTransportClient().prepareUpdate(indexname, typename, id).setDoc(mapping);
                } catch (NumberFormatException e) {
                    log.error("NumberFormatException error", e);
                } catch (IOException e) {
                    log.error("IOException error", e);
                }
                builder.add(urb);

                count++;

                // 每一千条提交一次
                if (count % 1000 == 0) {
                    BulkResponse bulkResponse = builder.get();
                    if (bulkResponse.hasFailures()) {
                        log.info(indexname + " update fail, " + bulkResponse.buildFailureMessage());
                    }

                    builder.request().requests().clear();
                    count = 0;
                }
				idstr = id + ",";
            }
            if (count > 0) {
                BulkResponse bulkResponse = builder.get();
                if (bulkResponse.hasFailures()) {
                    log.error("bulkUpdateIndexData has failures, msg = " + bulkResponse.buildFailureMessage());
                } else {
                    log.debug("bulkUpdateIndexData sucessfully!");
                }
            }
            // 跟踪调试
			if (dataMap.size() == 1) {
				Object utobj = dataMap.get("UPDATETIME");
				if (utobj != null) {
					long tt1 = System.currentTimeMillis();
					long tt2 = Long.valueOf(String.valueOf(utobj));
					if (tt1 - tt2 > 180000) {
						log.info(">>>>>>es Update time over 3 minutes, updatetime=" + tt2 + ", curtime=" + tt1
								+ ", idstr=" + idstr);
					}
				}
			}
        }catch(Exception ex) {
            log.error("bulkUpdateIndexData error!!!", ex);
        }
    }

    /**
     * 获取别名对应的索引
     * @param alias
     * @return
     */
    public List<String> getIndexsByAlias(String alias) {
        List<String> indexList = new ArrayList<String>();
        try {
            IndicesAdminClient indicesAdminClient = ESClientUtil.getInstance().getClient().admin().indices();
            GetAliasesResponse response = indicesAdminClient.prepareGetAliases(alias).get();
            ImmutableOpenMap<String, List<AliasMetaData>> aliasesMap = response.getAliases();
            Iterator<String> iterator = aliasesMap.keysIt();
            while (iterator.hasNext()) {
                String indexName = iterator.next();
                indexList.add(indexName);
            }
        } catch (Exception e) {
            log.error("getIndexsByAlias error, alias = " + alias);
        }

        return indexList;
    }
}

