package flink.elasticsearch;

import com.ultrapower.sigmamcloud.common.dubbo.RMIUtil;
import com.ultrapower.sigmamcloud.common.elasticsearch.ESClientUtil;
import com.ultrapower.sigmamcloud.common.elasticsearch.ESIndexProperty;
import com.ultrapower.sigmamcloud.common.elasticsearch.EsIndexDateUtil;
import com.ultrapower.sigmamcloud.common.elasticsearch.mapping.HistoryFaultIndexPropertyXmlConfig;
import com.ultrapower.sigmamcloud.common.elasticsearch.util.ESConstans;
import com.ultrapower.sigmamcloud.fault.model.Alert;
import com.ultrapower.sigmamcloud.fault.util.AlertCommonUtil;
import com.ultrapower.sigmamcloud.fault.util.AlertConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 历史告警入ES
 */
public class HistoryAlertESWriter {

    private TransportClient esClient;
    private Logger log = LoggerFactory.getLogger(HistoryAlertESWriter.class);

    public HistoryAlertESWriter() {
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
     * 在es中建立历史告警索引
     */
    public void saveHistoryAlert(Alert historyAlert) {
        try{
            // 设置告警的负责人的账号ID
            String guardian = historyAlert.getGuardian();
            String guardian_ids = historyAlert.getGuardian_Ids();
            if(StringUtils.isNotBlank(guardian) && StringUtils.isBlank(guardian_ids) ){
                List<Long> userIdlist = RMIUtil.getSecurityNewAPI().getUserIDListByUserAccounts(guardian, historyAlert.getAppId());
                if(null!=userIdlist){
                    historyAlert.setGuardian_Ids(userIdlist.stream().map(s->String.valueOf(s)).collect(Collectors.joining(",")));
                }
            }
        }catch (Exception ex){
        }

        try {
            long t1 = System.currentTimeMillis();

            if (esClient == null) {
                log.error("es client is null," + AlertCommonUtil.getAlertLogFormat(historyAlert));
                return;
            }

            // 将告警Alert对象转换为Map
            Map<String, Object> map = buildESMap(historyAlert);
            if(null==map || map.isEmpty()){
                log.error("buildESMap alert map is null, " + AlertCommonUtil.getAlertLogFormat(historyAlert));
            }

            BulkRequestBuilder bulkRequest = esClient.prepareBulk();
            long clrtime = historyAlert.getClearTime();
            String indexPrefix = ESConstans.getFmHistoryAlertIndex(historyAlert.getAppId());
            String indexName = EsIndexDateUtil.getInstance().getEsDateTableName(indexPrefix, clrtime);
            IndexRequest request = esClient.prepareIndex(indexName, ESConstans.INDEX_TYPE, historyAlert.getEventId() + "").setSource(map).request();
            bulkRequest.add(request);
            BulkResponse bulkResponse = bulkRequest.execute().actionGet();

            long t2 = System.currentTimeMillis();
            if (bulkResponse.hasFailures()) {
                log.error("save history Alert error," + AlertCommonUtil.getAlertLogFormat(historyAlert) + ",error message=" + bulkResponse.buildFailureMessage());
            } else {
                log.info("save history Alert finished," + AlertCommonUtil.getAlertLogFormat(historyAlert) + ",time=" + (t2 - t1));
            }
        } catch (Exception e) {
            log.error("create history Alert indexes error", e);
        }
    }

    /**
     * 生成es map对象
     */
    private Map<String, Object> buildESMap(Alert history) throws Exception {
        AlertConverter converter = new AlertConverter();
        // 将告警Alert对象转换为Map
        Map<String, String> alertPropertiesMap = converter.toMap(history,true);
        List<ESIndexProperty> indexProperties = HistoryFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexProperties == null || indexProperties.isEmpty()) {
            throw new Exception("es_fm_historyalert_mapping property list is empty");
        }

        Map<String, Object> map = new HashMap<>();
        try{
            for (ESIndexProperty indexPro : indexProperties) {
                String propertyTitle = indexPro.getName();
                String propertyType = indexPro.getType();

                String value = alertPropertiesMap.get(propertyTitle.toLowerCase());
                // 将历史告警中字符串值进行转换，其中字符串转换为小写
                Object obj = StringUtils.isBlank(value) ? value : getDocFieldValue(value, propertyType);

                map.put(propertyTitle, obj);
            }

            // 将mpointclass设置为正常的大小写
            map.put("MPOINTCLASS", history.getMpointClass());
            // 将mpointclass设置为正常的大小写
            map.put("CLASSNAME", history.getClassName());
        }catch(Exception ex){
            log.error("create history Alert map error", ex);
        }
        return map;
    }
}

