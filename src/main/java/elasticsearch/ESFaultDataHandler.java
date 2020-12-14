package elasticsearch;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 
 * @ClassName:     ESFaultDataHandler
 * @Description:   TODO(用一句话描述该文件做什么) 
 * 
 * @company        北京神州泰岳软件股份有限公司
 * @author         pengyue
 * @email          pengyue@ultrapower.com.cn
 * @version        V1.0
 * @Date           2020年3月23日 上午11:13:16
 */
public class ESFaultDataHandler extends ESDataHandler {

    private static Logger logger = LoggerFactory.getLogger(ESFaultDataHandler.class);

    
    
    /**
     * 根据条件， 获取所有查询结果列表  
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilders  排序条件
     * @return
     * @author caijinpeng
     */
    public List<Map<String, Object>> getResultsByQueryBuilderAndSort(String indexName, String typeName, QueryBuilder queryBuilder, List<FieldSortBuilder> sortBuilders) {
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        
        try {
        	SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            if (queryBuilder != null) {
                srb = srb.setQuery(queryBuilder);
            }
            if (null!=sortBuilders && sortBuilders.size()>0) {
            	for(int i=0; i<sortBuilders.size(); i++) {
            		srb = srb.addSort(sortBuilders.get(i));
            	}
                ///srb = srb.addSort(sortBuilder);
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
        	logger.error("getAllResults error!! ", ex);
        	ex.printStackTrace();
        }
        return results;
    }
    
    
    
    /**
     * 根据条件和排序条件，  获取查询结果列表 (带分页)
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilders  排序条件
     * @param form
     * @param size
     * @return
     * @author caijinpeng
     */
    public List<Map<String, Object>> getResultsByQueryBuilderAndSort(String indexName, String typeName, QueryBuilder queryBuilder, List<FieldSortBuilder> sortBuilders, int form,
                                                                     int size) {
    	
        if(indexName == null){
            return null;
        }
        
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
        	SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            srb = srb.setQuery(queryBuilder);
            if (null!=sortBuilders && sortBuilders.size()>0) {
            	for(int i=0; i<sortBuilders.size(); i++) {
            		srb = srb.addSort(sortBuilders.get(i));
            	}
                ///srb = srb.addSort(sortBuilder);
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
        	logger.error("getResultsByQueryBuilderAndSortBuilder error!!!", ex);
        	ex.printStackTrace();
        }
        return results;
    }
    
    
    /**
     * 根据条件和排序条件，  获取查询结果列表 (带分页) (多个索引)
     * 
     * @param indexName    索引数组 (indexName[] ) 
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilders  排序条件
     * @param form
     * @param size
     * @return
     * @author caijinpeng
     */
    public List<Map<String, Object>> getResultsByQueryBuilderAndSort(String[] indexName, String typeName, QueryBuilder queryBuilder, List<FieldSortBuilder> sortBuilders,
                                                                     int form, int size) {
        
        if(indexName == null){
            return null;
        }
        
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            TransportClient client = getTransportClient();
            SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(typeName);
            srb = srb.setQuery(queryBuilder);
            if (null!=sortBuilders && sortBuilders.size()>0) {
            	for(int i=0; i<sortBuilders.size(); i++) {
            		srb = srb.addSort(sortBuilders.get(i));
            	}
                ///srb = srb.addSort(sortBuilder);
            }
            
            if (form >= 10000) {
                int curpage = form / size;
                SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                for (int i = 0; i < curpage; i++) {
                    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    if (i == curpage - 1) {
                        Map<String, Object> map = null;
                        for (SearchHit hit : scrollResp.getHits()) {
                            map = hit.getSourceAsMap();
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
                        Map<String, Object> map = null;
                        for (SearchHit it : searchHits) {
                            map = it.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            results.add(map);
                        }
                    }
                }
            }
            
        } catch (Exception e) {
            logger.error("getResultsByQueryBuilderAndSortBuilder exception!", e);
            e.printStackTrace();
        }

        return results;
    }
    
    
    
    
    /**
     * 根据条件和排序条件， 获取查询结果列表， 并且只返回指定字段 (带分页)
     * 
     * @param indexName    索引名称
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilders  排序条件
     * @param fields       指定返回列数组字段
     * @param form
     * @param size
     * @return
     * @author caijinpeng
     */
    public List<Map<String, Object>> getShowFieldResultsByQueryBuilderAndSort(String indexName, String typeName, QueryBuilder queryBuilder, List<FieldSortBuilder> sortBuilders,
                                                                              String[] fields, int form, int size) {
        
        if(indexName == null){
            return null;
        }
        
        //如果没有指定返回列则返回全部列
        if(fields == null || fields.length <= 0){
            return getResultsByQueryBuilderAndSort(indexName, typeName, queryBuilder, sortBuilders, form, size);
        }
        
        
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            TransportClient client = getTransportClient();
            //指定返回列
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            FetchSourceContext sourceContext = new FetchSourceContext(true,fields,null);
            searchSourceBuilder.fetchSource(sourceContext);
          
            SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(typeName).setSource(searchSourceBuilder);
            srb = srb.setQuery(queryBuilder);
            if (null!=sortBuilders && sortBuilders.size()>0) {
            	for(int i=0; i<sortBuilders.size(); i++) {
            		srb = srb.addSort(sortBuilders.get(i));
            	}
                ///srb = srb.addSort(sortBuilder);
            }
            
            if (form >= 10000) {
                int curpage = form / size;
                SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                for (int i = 0; i < curpage; i++) {
                    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    if (i == curpage - 1) {
                        Map<String, Object> map = null;
                        for (SearchHit hit : scrollResp.getHits()) {
                            map = hit.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            if(!map.isEmpty()){
                                results.add(map);
                            }
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
                        Map<String, Object> map = null;
                        for (SearchHit hit : searchHits) {
                            map = hit.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            if(!map.isEmpty()){
                                results.add(map);
                            }
                        }
                    }
                }
            }
        
        } catch (Exception e) {
            logger.error("getResultFieldsByQueryBuilderAndSortBuilder exception!", e);
            e.printStackTrace();
        }

        return results;
    }
    
    

    /**
     * 根据条件和排序条件，  获取查询结果列表 (带分页) (多个索引)
     * 
     * @param indexName    索引数组 (indexName[] ) 
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilders  排序条件
     * @param fields       指定返回列数组字段
     * @param form
     * @param size
     * @return
     * @author caijinpeng
     */
    public List<Map<String, Object>> getShowFieldResultsByQueryBuilderAndSort(String[] indexName, String typeName, QueryBuilder queryBuilder, List<FieldSortBuilder> sortBuilders,
                                                                              String[] fields, int form, int size) {
        
        if(indexName == null){
            return null;
        }
        
        //如果没有指定返回列则返回全部列
        if(fields == null || fields.length <= 0){
            return getResultsByQueryBuilderAndSort(indexName, typeName, queryBuilder, sortBuilders, form, size);
        }


        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            TransportClient client = getTransportClient();
            //指定返回列
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            FetchSourceContext sourceContext = new FetchSourceContext(true,fields,null);
            searchSourceBuilder.fetchSource(sourceContext);
            SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(typeName).setSource(searchSourceBuilder);
            srb = srb.setQuery(queryBuilder);
            if (null!=sortBuilders && sortBuilders.size()>0) {
            	for(int i=0; i<sortBuilders.size(); i++) {
            		srb = srb.addSort(sortBuilders.get(i));
            	}
                ///srb = srb.addSort(sortBuilder);
            }
            
            
            if (form >= 10000) {
                int curpage = form / size;
                SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                for (int i = 0; i < curpage; i++) {
                    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    if (i == curpage - 1) {
                        Map<String, Object> map = null;
                        for (SearchHit hit : scrollResp.getHits()) {
                            map = hit.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            if(!map.isEmpty()){
                                results.add(map);
                            }
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
                        Map<String, Object> map = null;
                        for (SearchHit hit : searchHits) {
                            map = hit.getSourceAsMap();
                            if (map == null || map.isEmpty()) {
                                continue;
                            }
                            if(!map.isEmpty()){
                                results.add(map);
                            }
                        }
                    }
                }
            }
        
        } catch (Exception e) {
            logger.error("getResultFieldsByQueryBuilderAndSortBuilder exception!", e);
            e.printStackTrace();
        }

        return results;
    }
    
    
    
    /**
     * 根据条件和排序条件， 获取查询的SearchHits
     * @param indexName    索引数组 (indexName[] ) 
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilder  排序条件
     * @param form
     * @param size
     * @return
     */
    public SearchHits getSearchHitss(String[] indexName, String typeName, QueryBuilder queryBuilder, SortBuilder sortBuilder, int form, int size) {
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
            logger.error("getSearchHitss error!!!  condition >>>>>> " + srb.toString().replaceAll("\n", ""), ex);
            ex.printStackTrace();
        }
        return searchHits;
    }



    /**
     * 获取多个字段的统计结果
     * @param indexName    索引数组 (indexName[] )
     * @param typeName
     * @param filterBuilder
     * @param fields
     * @return
     * @author caijinpeng
     */
    public Map<String, Map<String, Long>> getAggregationss(String[] indexName, String typeName, QueryBuilder filterBuilder, String[] fields) {
        if (fields == null) {
            return null;
        }
        // 获取统计信息
        Map<String, Map<String, Long>> fieldsMap = new HashMap<String, Map<String, Long>>();
        try {
            SearchRequestBuilder srb = getTransportClient().prepareSearch(indexName).setTypes(typeName);
            // 查询条件
            if (filterBuilder != null) {
                srb = srb.setQuery(filterBuilder);
            }

            // 设置统计字段
            for (String field : fields) {
                if (field == null || field.equals("")) {
                    continue;
                }
                srb.addAggregation(AggregationBuilders.terms(field).field(field).size(8000));
            }

            logger.debug(">>>>>>> conditon >>>>>>>>>" + srb.toString().replaceAll("\n", ""));

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
        } catch (Exception e) {
            logger.error("getAggregationss error", e);
            e.printStackTrace();
        }
        return fieldsMap;
    }

}

