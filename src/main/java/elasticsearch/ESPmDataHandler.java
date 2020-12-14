package elasticsearch;

import com.ultrapower.fsms.common.elasticsearch.util.ESConstans;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * ES中pm性能数据查询处理
 * 
 * @Company: ultrapower.com
 * @author ljy
 * @create time：2016年9月18日 下午4:49:47
 * @version 1.0
 */
public class ESPmDataHandler extends ESDataHandler {

    private static Logger log = LoggerFactory.getLogger(ESPmDataHandler.class);

    /**
     * 聚合查询, 将一段时间内的性能数据集合聚合查询为某一时刻数据库(例如用于小时汇聚)
     * 
     * @param indexName    性能表名
     * @param startTime  开始时间
     * @param endTime    结束时间
     * @param time_id    集合后时间点
     * @return
     */
    public ArrayList<Map<String, String>> getAggreResult(String indexName, long startTime, long endTime, String time_id) {
        ArrayList<Map<String, String>> agglist = new ArrayList<Map<String, String>>();
        DecimalFormat df = new DecimalFormat("#0.00000");
        // 汇聚信息
        TermsAggregationBuilder kbpTermsBuilder = AggregationBuilders.terms("kbpAgg").field("KBP").size(10000);
        TermsAggregationBuilder kpiTermsBuilder = AggregationBuilders.terms("kpiAgg").field("KPI_NO").size(10000);
        StatsAggregationBuilder statsBuilder = AggregationBuilders.stats("valAgg").field("VALUE");
        kbpTermsBuilder.subAggregation(kpiTermsBuilder.subAggregation(statsBuilder));

        // 查询条件
        QueryBuilder dctimeBuilder = QueryBuilders.rangeQuery("DCTIME").gte(startTime).lt(endTime);
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(dctimeBuilder);

        SearchRequestBuilder srb = ESClientUtil.getInstance().getClient().prepareSearch(indexName).setTypes(ESConstans.INDEX_TYPE);
        srb.setQuery(boolQueryBuilder).addAggregation(kbpTermsBuilder);
        SearchResponse sr = srb.execute().actionGet();

        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        if (aggMap == null) {
            log.warn("aggMap is null,pmtable=" + indexName + ", startTime=" + startTime + ", endTime=" + endTime);
            return agglist;
        }
        LongTerms kbpTerms = (LongTerms) aggMap.get("kbpAgg");
        if (kbpTerms == null) {
            log.warn("kbpTerms is null,pmtable=" + indexName + ", startTime=" + startTime + ", endTime=" + endTime);
            return agglist;
        }
        Iterator<Bucket> kbpBucketIt = kbpTerms.getBuckets().iterator();
        while (kbpBucketIt.hasNext()) {
            Bucket kbpBucket = kbpBucketIt.next();
            String kbp = kbpBucket.getKeyAsString();

            LongTerms kpiTerms = (LongTerms) kbpBucket.getAggregations().asMap().get("kpiAgg");
            if (kpiTerms == null) {
                log.warn("kpiTerms is null,kbp=" + kbp + ", startTime=" + startTime + ", endTime=" + endTime);
                continue;
            }
            Iterator<Bucket> kpiBucketIt = kpiTerms.getBuckets().iterator();
            while (kpiBucketIt.hasNext()) {
                Bucket kpiBucket = kpiBucketIt.next();

                String kpino = kpiBucket.getKeyAsString();
                InternalStats internalStats = kpiBucket.getAggregations().get("valAgg");
                if (internalStats == null) {
                    log.warn("internalStats is null,kpino=" + kpino + ", startTime=" + startTime + ", endTime=" + endTime);
                    continue;
                }
                // System.out.println(kbpBucket.getKey() + "--" +kpiBucket.getKey() + " ,min=" + internalStats.getMin() +",
                // max="+internalStats.getMax()+", avg="+internalStats.getAvg()+", sum="+internalStats.getSum());

                Map<String,String> map = new HashMap<String,String>();
                map.put("KBP", kbp);
                map.put("KPI_NO", kpino);
                map.put("TIME_ID", time_id);
                if(internalStats.getMin() == Double.POSITIVE_INFINITY || internalStats.getMax() == Double.NEGATIVE_INFINITY) {
                	//如果最小、最大值是正负无穷大,说明该时间段内的聚合无数据，此时间数据不加入集合
                    log.info("getAggreResult: This time(" + time_id + ") no aggre value, this time data discarded.");
                    continue;
                }else {
                	map.put("VALUEMIN", df.format(internalStats.getMin()));
                	map.put("VALUEMAX", df.format(internalStats.getMax()));
                	map.put("VALUEAVG", df.format(internalStats.getAvg()));
                	map.put("VALUESUM", df.format(internalStats.getSum()));
                }
                agglist.add(map);
            }
        }
        return agglist;
    }
    
    /**
     * 聚合查询, 针对单一kpikbp将一段时间内的性能数据集合聚合查询为某一时刻数据库
     * 
     * @param indexName  性能表名
     * @param kpiNo      kpi
     * @param kbpNo      kbp
     * @param startTime  开始时间
     * @param endTime    结束时间
     * @param time_id    集合后时间点
     * @return
     */
    public Map<String, String> getAggreResultBySingleKpiKbp(String indexName, long kpiNo, long kbpNo, long startTime, long endTime, String time_id) {
    	Map<String,String> map = null;
    	
        DecimalFormat df = new DecimalFormat("#0.00000");
        // 汇聚信息
        TermsAggregationBuilder kbpTermsBuilder = AggregationBuilders.terms("kbpAgg").field("KBP").size(10000);
        TermsAggregationBuilder kpiTermsBuilder = AggregationBuilders.terms("kpiAgg").field("KPI_NO").size(10000);
        StatsAggregationBuilder statsBuilder = AggregationBuilders.stats("valAgg").field("VALUE");
        kbpTermsBuilder.subAggregation(kpiTermsBuilder.subAggregation(statsBuilder));

        // 查询条件
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("KPI_NO", kpiNo)).must(QueryBuilders.termQuery("KBP", kbpNo)).must(QueryBuilders.rangeQuery("DCTIME").gte(startTime).lt(endTime)));

        SearchRequestBuilder srb = ESClientUtil.getInstance().getClient().prepareSearch(indexName).setTypes(ESConstans.INDEX_TYPE);
        srb.setQuery(queryBuilder).addAggregation(kbpTermsBuilder);
        SearchResponse sr = srb.execute().actionGet();

        Map<String, Aggregation> aggMap = sr.getAggregations().asMap();
        if (aggMap == null) {
            log.warn("aggMap is null,pmtable=" + indexName + ", startTime=" + startTime + ", endTime=" + endTime);
            return map;
        }
        LongTerms kbpTerms = (LongTerms) aggMap.get("kbpAgg");
        if (kbpTerms == null) {
            log.warn("kbpTerms is null,pmtable=" + indexName + ", startTime=" + startTime + ", endTime=" + endTime);
            return map;
        }
        Iterator<Bucket> kbpBucketIt = kbpTerms.getBuckets().iterator();
        while (kbpBucketIt.hasNext()) {
            Bucket kbpBucket = kbpBucketIt.next();

            LongTerms kpiTerms = (LongTerms) kbpBucket.getAggregations().asMap().get("kpiAgg");
            if (kpiTerms == null) {
                log.warn("kpiTerms is null,kbpNo=" + kbpNo + ", startTime=" + startTime + ", endTime=" + endTime);
                continue;
            }
            Iterator<Bucket> kpiBucketIt = kpiTerms.getBuckets().iterator();
            while (kpiBucketIt.hasNext()) {
                Bucket kpiBucket = kpiBucketIt.next();

                InternalStats internalStats = kpiBucket.getAggregations().get("valAgg");
                if (internalStats == null) {
                    log.warn("internalStats is null,kpiNo=" + kpiNo + ", startTime=" + startTime + ", endTime=" + endTime);
                    continue;
                }

                map = new HashMap<String,String>();
                map.put("KBP", kbpNo + "");
                map.put("KPI_NO", kpiNo + "");
                map.put("TIME_ID", time_id);
                if(internalStats.getMin() == Double.POSITIVE_INFINITY || internalStats.getMax() == Double.NEGATIVE_INFINITY) {
                	//如果最小、最大值是正负无穷大,说明该时间段内的聚合无数据，此时间数据不加入集合
                    log.info("getAggreResultBySingleKpiKbp: This time(" + time_id + ") no aggre value, this time data discarded.");
                    continue;
                }else {
	                map.put("VALUEMIN", df.format(internalStats.getMin()));
	                map.put("VALUEMAX", df.format(internalStats.getMax()));
	                map.put("VALUEAVG", df.format(internalStats.getAvg()));
	                map.put("VALUESUM", df.format(internalStats.getSum()));
                }
            }
        }
        
        return map;
    }
    
    /**
     * 聚合查询, 将一段时间内的性能数据聚合查询为按分钟间隔粒度的数据
     * 
     * @param  indexName 索引表名
     * @param  kpiNo     kpi指标
     * @param  kbpNo     kbp标识
     * @param  startTime 开始时间
     * @param  endTime   结束时间
     * @param  timeSpan  时间粒度，单位为分钟, 0则按1分钟处理
     * @return
     */
    public ArrayList<Map<String, Object>> getAggreResultByTimeSpan_Minute(String indexName, long kpiNo, long kbpNo, long startTime, long endTime, int timeSpan, String groupId) {
    	DateHistogramInterval dateHistogramInterval = DateHistogramInterval.MINUTE;
    	if(timeSpan > 0) {
    		dateHistogramInterval = DateHistogramInterval.minutes(timeSpan);
    	}
    	
    	return getAggreResultByTimeSpan(indexName, kpiNo, kbpNo, startTime, endTime, dateHistogramInterval, groupId);
    }
    
    /**
     * 聚合查询, 将一段时间内的性能数据聚合查询为按小时间隔粒度的数据
     * 
     * @param  indexName 索引表名
     * @param  kpiNo     kpi指标
     * @param  kbpNo     kbp标识
     * @param  startTime 开始时间
     * @param  endTime   结束时间
     * @param  timeSpan  时间粒度，单位为小时, 0则按1小时处理
     * @return
     */
    public ArrayList<Map<String, Object>> getAggreResultByTimeSpan_Hour(String indexName, long kpiNo, long kbpNo, long startTime, long endTime, int timeSpan, String groupId,int offset,int limit) {
    	DateHistogramInterval dateHistogramInterval = DateHistogramInterval.HOUR;
    	if(timeSpan > 0) {
    		dateHistogramInterval = DateHistogramInterval.hours(timeSpan);
    	}
    	
    	return getAggreResultByTimeSpan(indexName, kpiNo, kbpNo, startTime, endTime, dateHistogramInterval, groupId,offset,limit);
    }
    
    /**
     * 聚合查询, 将一段时间内的性能数据聚合查询为按天间隔粒度的数据
     * 
     * @param  indexName 索引表名
     * @param  kpiNo     kpi指标
     * @param  kbpNo     kbp标识
     * @param  startTime 开始时间
     * @param  endTime   结束时间
     * @param  timeSpan  时间粒度，单位为天, 0则按1天处理
     * @return
     */
    public ArrayList<Map<String, Object>> getAggreResultByTimeSpan_Day(String indexName, long kpiNo, long kbpNo, long startTime, long endTime, int timeSpan, String groupId,int offset,int limit) {
    	DateHistogramInterval dateHistogramInterval = DateHistogramInterval.DAY;
    	if(timeSpan > 0) {
    		dateHistogramInterval = DateHistogramInterval.days(timeSpan);
    	}
    	
    	return getAggreResultByTimeSpan(indexName, kpiNo, kbpNo, startTime, endTime, dateHistogramInterval, groupId,offset,limit);
    }
    
    /**
     * 聚合查询, 将一段时间内的性能数据聚合查询为按某一时间间隔粒度的数据
     * 
     * @param  indexName 索引表名
     * @param  kpiNo     kpi指标
     * @param  kbpNo     kbp标识
     * @param  startTime 开始时间
     * @param  endTime   结束时间
     * @param  dateHistogramInterval  时间粒度，单位为分钟(例如：5)
     * @param  groupId   组id
     * @return
     */
    private ArrayList<Map<String, Object>> getAggreResultByTimeSpan(String indexName, long kpiNo, long kbpNo, long startTime, long endTime, DateHistogramInterval dateHistogramInterval, String groupId) {
        ArrayList<Map<String, Object>> agglist = new ArrayList<Map<String, Object>>();
        DecimalFormat df = new DecimalFormat("#0.00000");//修复小于0的数字，小数点前没有0
        log.info("getAggreResultByTimeSpan: begin ..., indexName=" + indexName + ", kpiNo=" + kpiNo + ", kbpNo=" + kbpNo + ", startTime=" + startTime + ", endTime=" + endTime);

        //设置查询条件、排序条件
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("KPI_NO", kpiNo)));
        if(kbpNo != 0L ){
            queryBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("KBP", kbpNo)));
        }
        if(groupId != null && !"".equals(groupId)) {
            queryBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("GROUPID", groupId)));
        }
        queryBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("DCTIME").gte(startTime).lt(endTime)));

        // Stats集合汇聚信息
        StatsAggregationBuilder statsBuilder = AggregationBuilders.stats("valAgg").field("VALUE");
        
        //时间聚合, 设置东八区时间
        DateHistogramAggregationBuilder dateAggBuilder = AggregationBuilders.dateHistogram("timeAgg").field("DCTIME");
        dateAggBuilder.dateHistogramInterval(dateHistogramInterval).timeZone(DateTimeZone.forID("Asia/Shanghai")).subAggregation(statsBuilder);
        //field.format("yyyy-MM");  
        //field.minDocCount(0);//强制返回空 buckets,既空的月份也返回  
        //field.extendedBounds(new ExtendedBounds("2014-01", "2014-12"));// Elasticsearch 默认只返回你的数
        
        //查询
        SearchRequestBuilder srb = ESClientUtil.getInstance().getClient().prepareSearch(indexName).setTypes(ESConstans.INDEX_TYPE);
        srb.setQuery(queryBuilder).addAggregation(dateAggBuilder);
        srb.addSort(SortBuilders.fieldSort("DCTIME").order(SortOrder.ASC));
        SearchResponse sr = srb.execute().actionGet();

        Histogram agg = sr.getAggregations().get("timeAgg");
        for (Histogram.Bucket entry : agg.getBuckets()) {
        	//此方法统一使用13位毫秒
            DateTime time = (DateTime) entry.getKey();    // Key
            long key = 0L;
            if(time != null) {
            	key = time.getMillis();
            }
            //String keyAsString = entry.getKeyAsString(); // Key as String
            
            InternalStats internalStats = entry.getAggregations().get("valAgg");
            if (internalStats == null) {
                log.warn("internalStats is null, kpino=" + kpiNo + ", startTime=" + startTime + ", endTime=" + endTime);
                continue;
            }

            Map<String, Object> map = new HashMap<String, Object>();
            map.put("KBP", kbpNo);
            map.put("KPI_NO", kpiNo);
            map.put("TIME_ID", key);
            if(internalStats.getMin() == Double.POSITIVE_INFINITY || internalStats.getMax() == Double.NEGATIVE_INFINITY) {
                //如果最小、最大值是正负无穷大,说明该时间段内的聚合无数据，此时间数据不加入集合
                log.info("getAggreResultByTimeSpan: This time(" + key + ") no aggre value, this time data discarded.");
                continue;
            }else {
	            map.put("VALUEMIN", df.format(internalStats.getMin()));
	            map.put("VALUEMAX", df.format(internalStats.getMax()));
	            map.put("VALUEAVG", df.format(internalStats.getAvg()));
	            map.put("VALUESUM", df.format(internalStats.getSum()));
            }
            agglist.add(map);
        }
 
        return agglist;
    }


    private ArrayList<Map<String, Object>> getAggreResultByTimeSpan(String indexName, long kpiNo, long kbpNo, long startTime, long endTime, DateHistogramInterval dateHistogramInterval, String groupId, int offset, int limit) {
        ArrayList<Map<String, Object>> agglist = new ArrayList<Map<String, Object>>();
        DecimalFormat df = new DecimalFormat("#0.00000");//修复小于0的数字，小数点前没有0
        log.info("getAggreResultByTimeSpan: begin ..., indexName=" + indexName + ", kpiNo=" + kpiNo + ", kbpNo=" + kbpNo + ", startTime=" + startTime + ", endTime=" + endTime);

        //设置查询条件、排序条件
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("KPI_NO", kpiNo)));
        if(kbpNo != 0L ){
            queryBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("KBP", kbpNo)));
        }
        if(groupId != null && !"".equals(groupId)) {
            queryBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("GROUPID", groupId)));
        }
        queryBuilder.filter(QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("DCTIME").gte(startTime).lt(endTime)));

        // Stats集合汇聚信息
        StatsAggregationBuilder statsBuilder = AggregationBuilders.stats("valAgg").field("VALUE");

        //时间聚合, 设置东八区时间
        DateHistogramAggregationBuilder dateAggBuilder = AggregationBuilders.dateHistogram("timeAgg").field("DCTIME");
        dateAggBuilder.dateHistogramInterval(dateHistogramInterval).timeZone(DateTimeZone.forID("Asia/Shanghai")).subAggregation(statsBuilder);
        //field.format("yyyy-MM");
        //field.minDocCount(0);//强制返回空 buckets,既空的月份也返回
        //field.extendedBounds(new ExtendedBounds("2014-01", "2014-12"));// Elasticsearch 默认只返回你的数

        //查询
        SearchRequestBuilder srb = ESClientUtil.getInstance().getClient().prepareSearch(indexName).setTypes(ESConstans.INDEX_TYPE);
        srb.setQuery(queryBuilder).addAggregation(dateAggBuilder);
        srb.addSort(SortBuilders.fieldSort("DCTIME").order(SortOrder.ASC));
        SearchResponse sr = srb.execute().actionGet();

        Histogram agg = sr.getAggregations().get("timeAgg");
        for (Histogram.Bucket entry : agg.getBuckets()) {
            //此方法统一使用13位毫秒
            DateTime time = (DateTime) entry.getKey();    // Key
            long key = 0L;
            if(time != null) {
                key = time.getMillis();
            }
            //String keyAsString = entry.getKeyAsString(); // Key as String

            InternalStats internalStats = entry.getAggregations().get("valAgg");
            if (internalStats == null) {
                log.warn("internalStats is null, kpino=" + kpiNo + ", startTime=" + startTime + ", endTime=" + endTime);
                continue;
            }

            Map<String, Object> map = new HashMap<String, Object>();
            map.put("KBP", kbpNo);
            map.put("KPI_NO", kpiNo);
            map.put("TIME_ID", key);
            if(internalStats.getMin() == Double.POSITIVE_INFINITY || internalStats.getMax() == Double.NEGATIVE_INFINITY) {
                //如果最小、最大值是正负无穷大,说明该时间段内的聚合无数据，此时间数据不加入集合
                log.info("getAggreResultByTimeSpan: This time(" + key + ") no aggre value, this time data discarded.");
                continue;
            }else {
                map.put("VALUEMIN", df.format(internalStats.getMin()));
                map.put("VALUEMAX", df.format(internalStats.getMax()));
                map.put("VALUEAVG", df.format(internalStats.getAvg()));
                map.put("VALUESUM", df.format(internalStats.getSum()));
            }
            agglist.add(map);
        }

        return agglist;
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
    @Override
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
            TermsAggregationBuilder fieldTermsBuilder = AggregationBuilders.terms("kbpAgg").field("KBP").size(10000);
            TopHitsAggregationBuilder topHitBuilder = AggregationBuilders.topHits("top_value").size(1).sort("VALUE", SortOrder.DESC);
            fieldTermsBuilder.subAggregation(topHitBuilder);

            srb.addAggregation(fieldTermsBuilder);
            if (form >= 10000) {
                int curpage = form / size;
                SearchResponse scrollResp = srb.setScroll(new TimeValue(60000)).setSize(size).execute().actionGet();
                for (int i = 0; i < curpage; i++) {
                    scrollResp =
                            getTransportClient().prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                    if (i == curpage - 1) {
                        Map<String, Aggregation> aggMap = scrollResp.getAggregations().asMap();
                        addAggResultWithIndexName(aggMap,indexName,results);

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
                Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
                addAggResultWithIndexName(aggMap,indexName,results);


                lop:while (true) {
                    searchResponse = getTransportClient().prepareSearchScroll(searchResponse.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();

                    searchHit = searchResponse.getHits();
                    cursize = searchHit.getHits().length;

                    // 再次查询不到数据时跳出循环
                    if (cursize == 0) {
                        break;
                    }
                    Map<String, Aggregation> aggMap1 = searchResponse.getAggregations().asMap();
                    addAggResultWithIndexName(aggMap1,indexName,results);
                }
            } else {
                SearchResponse searchResponse = srb.execute().actionGet();
                Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
                addAggResultWithIndexName(aggMap,indexName,results);
                total = searchResponse.getHits().getTotalHits().value;

            }
            resultMap.put("count",total);
            resultMap.put("data",results);
        }catch(Exception ex) {
            log.error("getResultsByQueryBuilderAndSortBuilder error!! ", ex);
        }
        return resultMap;
    }

    private void addAggResultWithIndexName(Map<String, Aggregation> aggMap, String indexName, List<Map<String,Object>> allResultList){
        List<Map<String,Object>> resultMapList = new ArrayList<>();
        LongTerms kbpTerms = (LongTerms) aggMap.get("kbpAgg");
        if (kbpTerms == null) {
            log.warn("kbpTerms is null,pmtable=" + indexName );
            return ;
        }

        Iterator<Bucket> kbpBucketIt = kbpTerms.getBuckets().iterator();
        while (kbpBucketIt.hasNext()) {
            Bucket kbpBucket = kbpBucketIt.next();
            String kbp = kbpBucket.getKeyAsString();

            TopHits topHits = (TopHits)kbpBucket.getAggregations().get("top_value");
            if (topHits == null) {
                log.warn("topHits is null,kbp=" + kbp );
                continue;
            }

            for (SearchHit hit : topHits.getHits().getHits()) {
                Map<String,Object> tmpMap = new HashMap<>();
                tmpMap.put("KBP", (String) hit.getSourceAsMap().get("KBP"));
                tmpMap.put("DCTIME", (String) hit.getSourceAsMap().get("DCTIME"));
                tmpMap.put("VALUE", (String) hit.getSourceAsMap().get("VALUE"));

                resultMapList.add(tmpMap);
            }

        }
        allResultList.addAll(resultMapList);
    }

    /*
     * 将时间转换为时间戳
     */
    private  String dateToStamp(String s){
        String res;
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
        Date date = null;
        try {
            date = sdf1.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        //calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8);
        long ts = date.getTime();
        res = calendar.getTimeInMillis()+"";
        return res;
    }


    /**
     * 获取最大dctime
     * 
     * @param indexName
     * @param typeName
     * @param queryBuilder
     * @return
     */
    public String getMaxDctime(String indexName, String typeName, BoolQueryBuilder queryBuilder) {
        MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max("maxDctime").field("DCTIME");
        SearchRequestBuilder srb = ESClientUtil.getInstance().getClient().prepareSearch(indexName).setTypes(ESConstans.INDEX_TYPE);
        srb.setQuery(queryBuilder).addAggregation(maxAggregationBuilder);
        SearchResponse sr = srb.execute().actionGet();
        Max agg = sr.getAggregations().get("maxDctime");
        String value = agg.getValueAsString();
        if (value.indexOf("E") > 0) {
            BigDecimal bd = new BigDecimal(value);
            value = bd.toPlainString();
        }
        return value;
    }
    
    /**
     * 判断索引中是否有数据
     * 
     * @param indexName
     * @return
     */
    @Override
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
     * 普通查询, 多索引获取查询结果(默认返回全部的列字段)
     * 
     * @param indexName    索引数组
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilder  排序条件
     * @param form
     * @param size
     * @return
     */
    public List<Map<String, Object>> getSearchResults(String[] indexName, String typeName, QueryBuilder queryBuilder, SortBuilder sortBuilder, int form, int size) {
        
    	List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        try {
            TransportClient client = getTransportClient();
            SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(typeName);
            srb = srb.setQuery(queryBuilder);
            if (sortBuilder != null) {
                srb = srb.addSort(sortBuilder);
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
            log.error("getSearchResults(): All columns get search results exception!", e);
            e.printStackTrace();
        }

        return results;
    }
    
    /**
     * 普通查询, 多索引获取查询结果 (可指定返回列)
     * 
     * @param indexName    索引数组
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilder  排序条件
     * @param form
     * @param size
     * @param fileds       指定返回列数组字段
     * @return
     */
    public List<Map<String, Object>> getSearchResults(String[] indexName, String typeName, QueryBuilder queryBuilder, SortBuilder sortBuilder, int form,
                                                      int size, String[] fileds) {
        
        if(indexName == null || indexName.length <= 0){
            return null;
        }
        
        //如果没有指定返回列则返回全部列
        if(fileds == null || fileds.length <= 0){
            return getSearchResults(indexName, typeName, queryBuilder, sortBuilder, form, size);
        }
        
        //指定返回列
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        FetchSourceContext sourceContext = new FetchSourceContext(true,fileds,null);
        searchSourceBuilder.fetchSource(sourceContext);
        TransportClient client = getTransportClient();
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(typeName).setSource(searchSourceBuilder);
        srb = srb.setQuery(queryBuilder);
        if (sortBuilder != null) {
            srb = srb.addSort(sortBuilder);
        }
        
        try {
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
            log.error("getSearchResults(): Custom columns get search results exception!", e);
            e.printStackTrace();
        }

        return results;
    }
    
    /**
     * 普通查询, 单个索引获取查询结果 (可指定返回列)
     * 
     * @param indexName    索引数组
     * @param typeName     索引类型
     * @param queryBuilder 查询条件
     * @param sortBuilder  排序条件
     * @param form
     * @param size
     * @param fileds       指定返回列数组字段
     * @return
     */
    public List<Map<String, Object>> getSearchResults(String indexName, String typeName, QueryBuilder queryBuilder, FieldSortBuilder sortBuilder, int form,
                                                      int size, String[] fileds) {
        
        if(indexName == null){
            return null;
        }
        
        //如果没有指定返回列则返回全部列
        if(fileds == null || fileds.length <= 0){
            return getResultsByQueryBuilder(indexName, typeName, queryBuilder, sortBuilder, form, size);
        }
        TransportClient client = getTransportClient();
        //指定返回列
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        FetchSourceContext sourceContext = new FetchSourceContext(true,fileds,null);
        searchSourceBuilder.fetchSource(sourceContext);
        List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
        SearchRequestBuilder srb = client.prepareSearch(indexName).setTypes(typeName).setSource(searchSourceBuilder);
        srb = srb.setQuery(queryBuilder);
        if (sortBuilder != null) {
            srb = srb.addSort(sortBuilder);
        }
        
        try {
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
            log.error("getSearchResults(): One indexname Custom columns get search results exception!", e);
            e.printStackTrace();
        }

        return results;
    }
    
}

