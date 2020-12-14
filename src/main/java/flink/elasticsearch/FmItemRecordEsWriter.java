package flink.elasticsearch;

import com.ultrapower.fsms.common.elasticsearch.ESFaultDataHandler;
import com.ultrapower.fsms.common.elasticsearch.ESIndexProperty;
import com.ultrapower.fsms.common.elasticsearch.EsIndexDateUtil;
import com.ultrapower.fsms.common.elasticsearch.mapping.ItemFaultIndexPropertyXmlConfig;
import com.ultrapower.fsms.common.elasticsearch.util.ESConstans;
import com.ultrapower.fsms.common.utils.ConverUtils;
import com.ultrapower.fsms.common.utils.DateTimeUtil;
import com.ultrapower.fsms.fault.model.Alert;
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
 * 
 * @ClassName:     FmItemRecordEsWriter
 * @Description:   告警派单工单记录向ES中写入
 * 
 * @company        北京神州泰岳软件股份有限公司
 * @author         caijinpeng
 * @email          caijinpeng@ultrapower.com.cn
 * @version        V1.0
 * @Date           2018年8月31日 下午2:23:01
 */
public class FmItemRecordEsWriter {

	private TransportClient esClient;
    private Logger log = LoggerFactory.getLogger(FmItemRecordEsWriter.class);

    public FmItemRecordEsWriter() {
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
     * 在es中保存工单记录
     * @param itemRecord
     * @return
     * @throws Exception
     */
    public boolean saveOrUpdateFmItemRecord(FmItemRecord itemRecord) throws Exception{
    	try {
			Map<String, Object> map = buildESMap(itemRecord);
            if(null==map) {
            	return false;
            }
            
            if (esClient == null) {
                log.error("es client is null, itemRecord save itemId: "+itemRecord.getItemId()+", eventId: "+itemRecord.getEventId());
                return false;
            }

            BulkRequestBuilder bulkRequest = esClient.prepareBulk();
            long startTime = itemRecord.getDispatchTime();
            
            String indexPrefix = ESConstans.getFmItemRecordIndex(itemRecord.getAppId());
            String indexName = EsIndexDateUtil.getInstance().getEsDateTableName(indexPrefix, startTime);
            
            if(null!=map) {
            	 long t1 = System.currentTimeMillis();

         		 IndexRequest request = esClient.prepareIndex(indexName, ESConstans.INDEX_TYPE, itemRecord.getItemId() + "").setSource(map).request();
                 bulkRequest.add(request);
                 BulkResponse bulkResponse = bulkRequest.execute().actionGet();

                 long t2 = System.currentTimeMillis();
                 if (bulkResponse.hasFailures()) {
                     log.error("save error, itemRecord eventId:" + itemRecord.getEventId() +", DISPATCHOPTUSER:"+ map.get("DISPATCHOPTUSER")+", DISPATCHSTATUS:"+ map.get("DISPATCHSTATUS") + ",  error message=" + bulkResponse.buildFailureMessage());
                 } else {
                     log.info("save finished, itemRecord eventId:" + itemRecord.getEventId()+", DISPATCHOPTUSER:"+ map.get("DISPATCHOPTUSER")+", DISPATCHSTATUS:"+ map.get("DISPATCHSTATUS") + ", time=" + (t2 - t1));
                     return true;
                 }
            }
        } catch (Exception e) {
            log.error("saveOrUpdateFmItemRecord  error", e);
        }
		return false;
    }


    /**
     * 在es中根据id，更新工单记录部分字段
     * @param indexName
     * @param itemId
     * @param map
     * @return
     */
    public boolean updateFmItemRecordPart(String indexName, String itemId, Map<String, Object> map){
        try {
            if(StringUtils.isBlank(itemId) || null==map) {
                return false;
            }

            if (esClient == null) {
                log.error("es client is null, itemRecord update itemId: "+ itemId);
                return false;
            }

            BulkRequestBuilder bulkRequest = esClient.prepareBulk();

            if(null!=map && !map.isEmpty()) {
                long t1 = System.currentTimeMillis();

                XContentBuilder source = XContentFactory.jsonBuilder().map(map);

                UpdateRequestBuilder urb = esClient.prepareUpdate(indexName, ESConstans.INDEX_TYPE, itemId);
                urb.setDoc(source);
                //urb.setDetectNoop(false);
                bulkRequest.add(urb);
                BulkResponse bulkResponse = bulkRequest.execute().actionGet();

                long t2 = System.currentTimeMillis();
                if (bulkResponse.hasFailures()) {
                    log.error("update error, itemRecord itemId:" + itemId + ",  error message=" + bulkResponse.buildFailureMessage());
                } else {
                    log.info("update finished, itemRecord itemId:" + itemId+ ", time=" + (t2 - t1));
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("updateFmItemRecordPart error", e);
        }
        return false;
    }



    /***
     * 标记告警的工单记录 是历史告警
     * @param alert
     * @param cleanTime
     * @return
     */
    public boolean flagFmItemRecordAlertClean(Alert alert, long cleanTime){
        boolean result = false;
        try{
            if (null==alert) {
                return false;
            }
            if (esClient == null) {
                log.error("es client is null, flag itemRecord alert clean, eventid: "+ alert.getEventId());
                return false;
            }

            long t1 = System.currentTimeMillis();

            BulkRequestBuilder bulkRequest = esClient.prepareBulk();

            // 先查询告警的派单记录
            List<Map<String, Object>>  list = getFmItemRecordByAlert(alert.getAppId(), alert, 0, 100);
            if(null!=list && list.size()>0){

                long t2 = System.currentTimeMillis();

                for(Map<String, Object> nmap : list){
                    if(null==nmap || nmap.isEmpty()){
                        continue;
                    }

                    String itemId = ConverUtils.Obj2Str(nmap.get("ITEMID"), "");
                    long dispatchTime = ConverUtils.Obj2long(nmap.get("DISPATCHTIME"));
                    if(StringUtils.isBlank(itemId) || dispatchTime==0){
                        continue;
                    }

                    String indexAlias = ESConstans.getFmItemRecordIndex(alert.getAppId());
                    String indexName = EsIndexDateUtil.getInstance().getEsDateTableName(indexAlias, dispatchTime);

                    Map<String, Object> map = new HashMap<>();
                    map.put("ALERT_CLEARTIME", cleanTime);
                    XContentBuilder source = XContentFactory.jsonBuilder().map(map);

                    UpdateRequestBuilder urb = esClient.prepareUpdate(indexName, ESConstans.INDEX_TYPE, itemId+"");
                    urb.setDoc(source);
                    //urb.setDetectNoop(false);
                    bulkRequest.add(urb);
                }

                BulkResponse bulkResponse = bulkRequest.execute().actionGet();

                long t3 = System.currentTimeMillis();
                if (bulkResponse.hasFailures()) {
                    log.error("update error, flag itemRecord alert clean, eventid: "+ alert.getEventId() + ",  error message=" + bulkResponse.buildFailureMessage());
                } else {
                    log.info("update finished, flag itemRecord alert clean, eventid: "+ alert.getEventId()+ ", time=" + (t3 - t1));
                    return true;
                }
            }
        }catch(Exception ex){
            log.error("flagFmItemRecordAlertClean error", ex);
        }
        return result;
    }



    /**
     * 根据告警alert，获取告警的派单信息
     */
    private List<Map<String, Object>> getFmItemRecordByAlert(String appId, Alert alert, int offset, int limit) {
        try {
            if(null==alert) {
                return null;
            }

            long occurTime = alert.getOccurTime();
            int diffMonth = DateTimeUtil.millisDiffToMonth(occurTime, System.currentTimeMillis());
            int diffMonths = diffMonth +1;


            // 获取近几月的ES的告警派单记录表的索引
            FmItemRecordUtil fmItemRecordBeanUtil = new FmItemRecordUtil();
            String[] indexNames = fmItemRecordBeanUtil.getFmItemRecordEsIndexNames(appId, diffMonths);

            if(offset==0 && limit==0) {
                offset = 0;
                limit = 50;
            }

            BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("APPID", appId))
                    .must(QueryBuilders.termQuery("EVENTID",  alert.getEventId()))
                    .must(QueryBuilders.termQuery("ALERT_CLEARTIME", 0));


            // 排序条件
            List<FieldSortBuilder> sortBuilders = new ArrayList<FieldSortBuilder>();
            FieldSortBuilder sortBuilder = SortBuilders.fieldSort("DISPATCHTIME")
                    .order(SortOrder.DESC);
            sortBuilders.add(sortBuilder);


            log.debug("queryBuilder >>>: "+queryBuilder.toString());

            // 执行查询
            ESFaultDataHandler handler = new ESFaultDataHandler();
            List<Map<String, Object>> result = handler.getResultsByQueryBuilderAndSort(indexNames, ESConstans.INDEX_TYPE, queryBuilder, sortBuilders, offset, limit);
            return result;
        }catch(Exception ex) {
            log.error("getFmItemRecordByEventId error!!!", ex);
            ex.printStackTrace();
        }
        return null;
    }


	/**
     * 生成es map对象
     */
    private Map<String, Object> buildESMap(FmItemRecord itemRecord) throws Exception {
        FmItemRecordUtil converter = new FmItemRecordUtil();
    	Map<String, String> shieldPropertiesMap = converter.toMap(itemRecord, true);
        if(null==shieldPropertiesMap) {
        	return null;
        }
        
        List<ESIndexProperty> indexProperties = ItemFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
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

