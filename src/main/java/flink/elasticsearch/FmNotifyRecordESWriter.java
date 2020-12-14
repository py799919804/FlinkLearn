package flink.elasticsearch;

import com.ultrapower.sigmamcloud.common.elasticsearch.ESClientUtil;
import com.ultrapower.sigmamcloud.common.elasticsearch.ESFaultDataHandler;
import com.ultrapower.sigmamcloud.common.elasticsearch.ESIndexProperty;
import com.ultrapower.sigmamcloud.common.elasticsearch.EsIndexDateUtil;
import com.ultrapower.sigmamcloud.common.elasticsearch.mapping.NotifyFaultIndexPropertyXmlConfig;
import com.ultrapower.sigmamcloud.common.elasticsearch.util.ESConstans;
import com.ultrapower.sigmamcloud.common.util.ConverUtils;
import com.ultrapower.sigmamcloud.common.util.DateTimeUtil;
import com.ultrapower.sigmamcloud.fault.model.Alert;
import com.ultrapower.sigmamcloud.fault.model.FmNotifyRecord;
import com.ultrapower.sigmamcloud.fault.util.FmNotifyRecordUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 告警通知的通知记录向ES中写入
 */
public class FmNotifyRecordESWriter {

    private TransportClient esClient;
    private Logger log = LoggerFactory.getLogger(FmNotifyRecordESWriter.class);

    public FmNotifyRecordESWriter() {
        esClient =  ESClientUtil.getInstance().getClient();
    }


    /**
     * 值类型转换
     */
    private Object getDocFieldValue(String value, String fieldType) {
        // 转换成int
        if (fieldType.equalsIgnoreCase(ESConstans.FieldType.INTEGER)) {
            int i = 0;
            try {
                i = NumberUtils.toInt(value, 0);
            } catch (Exception ignored) {}

            return i;
        }

        // 转换成long
        if (fieldType.equalsIgnoreCase(ESConstans.FieldType.LONG)) {
            long l = 0;
            try {
                l = NumberUtils.toLong(value, 0);
            } catch (Exception ignored) {}

            return l;
        }

        // 将字符串内容转换为小写
        if(value==null) {
            value = "";
        }
        return StringUtils.lowerCase(value);
    }



    /**
     * 在es中保存通知记录
     * @param notifyRecord
     * @return
     * @throws Exception
     */
    public boolean saveOrUpdateFmNotifyRecord(FmNotifyRecord notifyRecord) throws Exception{
        try {
            Map<String, Object> map = buildESMap(notifyRecord);
            if(null==map) {
                return false;
            }

            if (esClient == null) {
                log.error("es client is null, notifyRecord save notifyId: "+notifyRecord.getNotifyId()+", eventId: "+notifyRecord.getEventId());
                return false;
            }

            BulkRequestBuilder bulkRequest = esClient.prepareBulk();
            long startTime = notifyRecord.getNotifyTime();

            String indexPrefix = ESConstans.getFmNotifyRecordIndex(notifyRecord.getAppId());
            String indexName = EsIndexDateUtil.getInstance().getEsDateTableName(indexPrefix, startTime);

            if(null!=map) {
                long t1 = System.currentTimeMillis();

                IndexRequest request = esClient.prepareIndex(indexName, ESConstans.INDEX_TYPE, notifyRecord.getNotifyId() + "").setSource(map).request();
                bulkRequest.add(request);
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();

                long t2 = System.currentTimeMillis();
                if (bulkResponse.hasFailures()) {
                    log.error("save error, notifyRecord eventId:" + notifyRecord.getEventId() +", NOTIFYTIME:"+ map.get("NOTIFYTIME")+", METHOD:"+ map.get("METHOD") + ",  error message=" + bulkResponse.buildFailureMessage());
                } else {
                    log.info("save finished, notifyRecord eventId:" + notifyRecord.getEventId()+", NOTIFYTIME:"+ map.get("NOTIFYTIME")+", METHOD:"+ map.get("METHOD") + ", time=" + (t2 - t1));
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("saveOrUpdateFmNotifyRecord  error", e);
        }
        return false;
    }


    /**
     * 在es中根据id，更新通知记录部分字段
     * @param indexName
     * @param notifyId
     * @param map
     * @return
     */
    public boolean updateFmNotifyRecordPart(String indexName, String notifyId, Map<String, Object> map){
        try {
            if(StringUtils.isBlank(notifyId) || null==map) {
                return false;
            }

            if (esClient == null) {
                log.error("es client is null, notifyRecord update notifyId: "+ notifyId);
                return false;
            }

            BulkRequestBuilder bulkRequest = esClient.prepareBulk();

            if(null!=map && !map.isEmpty()) {
                long t1 = System.currentTimeMillis();

                XContentBuilder source = XContentFactory.jsonBuilder().map(map);

                UpdateRequestBuilder urb = esClient.prepareUpdate(indexName, ESConstans.INDEX_TYPE, notifyId);
                urb.setDoc(source);
                //urb.setDetectNoop(false);
                bulkRequest.add(urb);
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();

                long t2 = System.currentTimeMillis();
                if (bulkResponse.hasFailures()) {
                    log.error("update error, notifyRecord notifyId:" + notifyId + ",  error message=" + bulkResponse.buildFailureMessage());
                } else {
                    log.info("update finished, notifyRecord notifyId:" + notifyId+ ", time=" + (t2 - t1));
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("updateFmNotifyRecordPart error", e);
        }
        return false;
    }

    /***
     * 标记告警的通知记录 是历史告警
     * @param alert
     * @param cleanTime
     * @return
     */
    public boolean flagFmNotifyRecordAlertClean(Alert alert, long cleanTime){
        boolean result = false;
        try{
            if (null==alert) {
                return false;
            }
            if (esClient == null) {
                log.error("es client is null, flag notifyRecord alert clean, eventid: "+ alert.getEventId());
                return false;
            }

            long t1 = System.currentTimeMillis();

            BulkRequestBuilder bulkRequest = esClient.prepareBulk();

            // 先查询告警的通知记录
            List<Map<String, Object>>  list = getFmNotifyRecordsByEventID(alert.getAppId(), alert, 0, 100);
            if(null!=list && list.size()>0){

                long t2 = System.currentTimeMillis();

                for(Map<String, Object> nmap : list){
                    if(null==nmap || nmap.isEmpty()){
                        continue;
                    }

                    String notifyId = ConverUtils.Obj2Str(nmap.get("NOTIFYID"), "");
                    long notifyTime = ConverUtils.Obj2long(nmap.get("NOTIFYTIME"));
                    if(StringUtils.isBlank(notifyId) || notifyTime==0){
                        continue;
                    }

                    String indexAlias = ESConstans.getFmNotifyRecordIndex(alert.getAppId());
                    String indexName = EsIndexDateUtil.getInstance().getEsDateTableName(indexAlias, notifyTime);

                    Map<String, Object> map = new HashMap<>();
                    map.put("ALERT_CLEARTIME", cleanTime);
                    XContentBuilder source = XContentFactory.jsonBuilder().map(map);

                    UpdateRequestBuilder urb = esClient.prepareUpdate(indexName, ESConstans.INDEX_TYPE, notifyId+"");
                    urb.setDoc(source);
                    //urb.setDetectNoop(false);
                    bulkRequest.add(urb);
                }

                BulkResponse bulkResponse = bulkRequest.execute().actionGet();

                long t3 = System.currentTimeMillis();
                if (bulkResponse.hasFailures()) {
                    log.error("update error, flag notifyRecord alert clean, eventid: "+ alert.getEventId() + ",  error message=" + bulkResponse.buildFailureMessage());
                } else {
                    log.info("update finished, flag notifyRecord alert clean, eventid: "+ alert.getEventId()+ ", time=" + (t3 - t1));
                    return true;
                }
            }
        }catch(Exception ex){
            log.error("flagFmNotifyRecordAlertClean error", ex);
        }
        return result;
    }


    /**
     * 根据告警ID(eventID)，获取告警的通知记录的列表
     */
    private List<Map<String, Object>> getFmNotifyRecordsByEventID(String appId, Alert alert, int offset, int limit) {
        try {
            if(null==alert) {
                return null;
            }

            long occurTime = alert.getOccurTime();
            int diffMonth = DateTimeUtil.millisDiffToMonth(occurTime, System.currentTimeMillis());
            int diffMonths = diffMonth +1;

            // 获取近几月的ES的告警通知记录表的索引
            FmNotifyRecordUtil fmNotifyRecordUtil = new FmNotifyRecordUtil();
            String[] indexNames = fmNotifyRecordUtil.getFmNotifyRecordEsIndexNames(appId, diffMonths);

            if(offset==0 && limit==0) {
                offset = 0;
                limit = 50;
            }

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("APPID", appId))
                    .must(QueryBuilders.termQuery("EVENTID", alert.getEventId()))
                    .must(QueryBuilders.termQuery("ALERT_CLEARTIME", 0));

            // 排序条件
            List<FieldSortBuilder> sortBuilders = new ArrayList<FieldSortBuilder>();
            FieldSortBuilder sortBuilder = SortBuilders.fieldSort("NOTIFYTIME")
                    .order(SortOrder.DESC);
            sortBuilders.add(sortBuilder);


            log.debug("queryBuilder >>>: "+queryBuilder.toString());

            // 执行查询
            ESFaultDataHandler handler = new ESFaultDataHandler();
            List<Map<String, Object>> result = handler.getResultsByQueryBuilderAndSort(indexNames, ESConstans.INDEX_TYPE, queryBuilder, sortBuilders, offset, limit);
            return result;
        }catch(Exception ex) {
            log.error("getFmNotifyRecordsByEventID error!!!", ex);
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * 生成es map对象
     */
    private Map<String, Object> buildESMap(FmNotifyRecord notifyRecord) throws Exception {
        FmNotifyRecordUtil converter = new FmNotifyRecordUtil();
        Map<String, String> shieldPropertiesMap = converter.toMap(notifyRecord, true);
        if(null==shieldPropertiesMap) {
            return null;
        }

        List<ESIndexProperty> indexProperties = NotifyFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexProperties == null || indexProperties.isEmpty()) {
            throw new Exception("property list is empty");
        }


        Map<String, Object> map = new HashMap<String, Object>();
        for (ESIndexProperty indexPro : indexProperties) {
            String propertyTitle = indexPro.getName();
            String propertyType = indexPro.getType();

            String value = shieldPropertiesMap.get(propertyTitle.toUpperCase());
            Object obj = StringUtils.isBlank(value) ? value : getDocFieldValue(value, propertyType);

            map.put(propertyTitle, obj);
        }

        return map;
    }

}

