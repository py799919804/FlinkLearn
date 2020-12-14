package elasticsearch;

import com.ultrapower.fsms.common.elasticsearch.mapping.*;
import com.ultrapower.fsms.common.elasticsearch.util.ESConstans;
import com.ultrapower.fsms.common.utils.DateTimeUtil;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Title: ES mapping定义
 * @description:
 *
 * @Company: ultrapower.com
 * @author lnj2050
 * @create time：2016年5月12日 下午1:58:34
 * @version 1.0
 */
public class ESMappingManager {
    public final static String ESMAPPING_PM = "pm";
    public final static String ESMAPPING_RESOURCE = "resource";
    public final static String ESMAPPING_FM = "fault";
    public final static String ESMAPPING_RD = "rawData";
    public final static String ESMAPPING_OR = "operateRecord";
    public final static String ESMAPPING_CM = "cm";
    public final static String ESMAPPING_ABNORMAL = "abnormal";
    public final static String ESMAPPING_PROCESS_TRACE = "processtrace";
    public final static String ESMAPPING_ANALYZE_LOG = "analyzelog";
    // 默认提前创建3天或3个月
    public final static int INDEXSIZE = 3;
    // 日志
    private static Logger log = LoggerFactory.getLogger(ESMappingManager.class);

    /**
     * 创建cm索引
     * @param indexAlias
     * @param needUpdate
     */
	public void createCMIndexMapping(String indexAlias, boolean needUpdate) {
		List<ESIndexProperty> indexPropertyList = ESCmIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
		String curIndexName = indexAlias + "_000000";
		createIndexMapping(curIndexName, ESConstans.INDEX_TYPE, indexPropertyList, needUpdate, indexAlias);
	}

    /**
     * 创建pm索引
     * @param indexname
     * @param needUpdate
     */
	public void createPMIndexMapping(String indexname, boolean needUpdate) {
		String newIndexName = indexname.toLowerCase();
		String classtitle = newIndexName.substring(newIndexName.lastIndexOf("_") + 1);
		List<ESIndexProperty> indexPropertyList = ESPmIndexPropertyXmlConfig.getInstance()
				.getPmIndexPropertyListByClassTitle(classtitle);
		if (indexPropertyList == null || indexPropertyList.size() <= 0) {
			log.error("can't get indexPropertyList by classtitle=" + classtitle);
			return;
		}
		// 这里增加判断 看看是否需要进行下一天索引的创建
		priorCreateIndexesMapping(newIndexName, indexPropertyList, needUpdate);
	}

	public void createKpiIndexMapping(String tableName, boolean needUpdate, String kpiType) {
		// 2019.1.18改造（cm数据不删除，只创建一个索引。因为cm数据只插入一次，值没变化不再插入）
		if (kpiType != null && kpiType.equals("CM")) {
			createCMIndexMapping(tableName, needUpdate);
		} else {
			createPMIndexMapping(tableName, needUpdate);
		}
	}

    /**
     * 创建原始性能数据索引mapping
     *
     * @param indexname
     * @@param needUpdate 索引如果已经存在的话 是否需要更新
     */
//    public void createPCIndexMapping(String indexname, boolean needUpdate, String kpiType) {
//        String newIndexName = indexname.toLowerCase();
//        String classtitle = newIndexName.substring(newIndexName.lastIndexOf("_") + 1);
//        List<ESIndexProperty> indexPropertyList = null;
//        if (kpiType != null && kpiType.equals("CM")) { // cm
//            indexPropertyList = ESCmIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
//        } else { // pm
//            indexPropertyList = ESPmIndexPropertyXmlConfig.getInstance().getPmIndexPropertyListByClassTitle(classtitle);
//        }
//        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
//            log.error("can't get indexPropertyList by classtitle=" + classtitle);
//            return;
//        }
//        // 这里增加判断 看看是否需要进行下一天索引的创建
//        priorCreateIndexesMapping(newIndexName, indexPropertyList, needUpdate);
//    }

    /**
     * 实现提前几天(月)的索引进行创建
     * @param indexAlias
     * @param indexPropertyList
     * @param needUpdate
     */
    private void priorCreateIndexesMapping(String indexAlias, List<ESIndexProperty> indexPropertyList, boolean needUpdate) {
        long curdctime = System.currentTimeMillis();
        String curIndexName = EsIndexDateUtil.getInstance().getEsDateTableName(indexAlias, curdctime);
        createIndexMapping(curIndexName, ESConstans.INDEX_TYPE, indexPropertyList, needUpdate, indexAlias);
        long dctime = 0;
        String curnewIndexName = null;
        for (int i = 0; i < INDEXSIZE; i++) { // 说明是按月存储的
            try{
                if (curIndexName.endsWith("00")) {
                    dctime = DateTimeUtil.getFirstDayMillsOfMonth(i + 1);// curdctime + (i+1)*30*24 * 60 * 60 * 1000L;//月初创建有问题
                } else {
                    dctime = curdctime + (i + 1) * 24 * 60 * 60 * 1000L;
                }
                curnewIndexName = EsIndexDateUtil.getInstance().getEsDateTableName(indexAlias, dctime);
                // log.info("curnewIndexName--------------"+ curnewIndexName);
                if (!curIndexName.equalsIgnoreCase(curnewIndexName)) {
                    createIndexMapping(curnewIndexName, ESConstans.INDEX_TYPE, indexPropertyList, needUpdate, indexAlias);

                }
            }catch (Exception ex){
                log.error("priorCreateIndexesMapping create error! curIndexName="+curIndexName,  ex);
            }

            try {
                Thread.sleep(2);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    /**
     * 创建索引
     *
     * @param indexname
     * @param typename
     * @param indexPropertyList
     */
    public void createIndexMapping(String indexname, String typename, List<ESIndexProperty> indexPropertyList, boolean needUpdate, String alias) {
		IndicesAdminClient indicesAdminClient = null;
		try {
			TransportClient client = ESClientUtil.getInstance().getClient();
			indicesAdminClient = client.admin().indices();
		} catch (Exception e1) {
			log.error("get IndicesAdminClient error ,indexname=" + indexname, e1);
		}
		if (indicesAdminClient == null) {
			log.warn("IndicesAdminClient is null");
			return;
		}

		// 创建空索引（类似oracle中建表）
		boolean isexists = false;
		try {
			isexists = indicesAdminClient.prepareExists(indexname).get().isExists();
		} catch (Exception e1) {
			log.error("prepareExists error ,indexname=" + indexname, e1);
			return;
		}

		if (isexists) {
			if (needUpdate) {
				updateIndexField(indicesAdminClient, indexname, typename, indexPropertyList);
			} else {
				log.info(" createIndexMapping indexname=" + indexname + ",aready  exists");
				return;
			}
		} else {
			createNewIndex(indicesAdminClient, indexname, typename, indexPropertyList, alias);
		}
    }

    /**
     * 创建新索引
     * @param indicesAdminClient
     * @param indexname
     * @param typename
     * @param indexPropertyList
     * @param alias
     */
	public void createNewIndex(IndicesAdminClient indicesAdminClient, String indexname, String typename,
                               List<ESIndexProperty> indexPropertyList, String alias) {
		if (indexPropertyList == null || indexPropertyList.size() == 0) {
			return;
		}
		try {
			CreateIndexRequestBuilder builder = indicesAdminClient.prepareCreate(indexname);
			if (alias != null && !alias.equals(indexname)) {
				Alias aliasobj = new Alias(alias);
				builder.addAlias(aliasobj);
			}
			builder.execute().actionGet();
		} catch (Exception e) {
			log.error("create index error,indexname=" + indexname + ",alias=" + alias, e);
			return;
		}

		XContentBuilder mapping = null;
		try {
			List<ESIndexProperty> newProList = indexPropertyList;
			mapping = XContentFactory.jsonBuilder().startObject().startObject(typename); // 必须是类型名称
			mapping.startObject("_all").field("enabled", "false").endObject();
            mapping.startObject("properties");
            for (int i = 0; i < newProList.size(); i++) {
                ESIndexProperty indexProperty = indexPropertyList.get(i);
                String name = indexProperty.getName();
                String index = indexProperty.getIndex();
                String type = indexProperty.getType();
                mapping = mapping.startObject(name).field("type", type).field("index", index).endObject();
            }
            mapping = mapping.endObject().endObject().endObject();
		} catch (Exception tr) {
			log.error("get mapping error, indexname=" + indexname + ",typename=" + typename, tr);
			return;
		}
		if (mapping == null) {
			log.warn("mapping is null, indexname=" + indexname);
			return;
		}

		try {
			PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
			indicesAdminClient.putMapping(mappingRequest).actionGet();
		} catch (Exception tr) {
			log.error("createIndexMapping indexname=" + indexname + ",typename=" + typename + " catch an exception",
					tr);
			try {
				indicesAdminClient.prepareDelete(indexname).execute().actionGet();
			} catch (Exception e1) {
				log.error("delete index error ,indexname=" + indexname, e1);
			}
			return;
		}
		// 已经创建的索引存储在redis中
		new ESIndexRDA().insertESIndex2Redis(alias);
		log.info("createIndexMapping indexname=" + indexname + " successfully!");
	}

	/**
	 * 更新索引字段
	 *
	 * @param indicesAdminClient
	 * @param indexname
	 * @param typename
	 * @param indexPropertyList
	 */
	public void updateIndexField(IndicesAdminClient indicesAdminClient, String indexname, String typename,
                                 List<ESIndexProperty> indexPropertyList) {
		XContentBuilder mapping = null;
		try {
			List<ESIndexProperty> newProList = getUpdateFieldList(indexname, typename, indexPropertyList);
			if (newProList != null && newProList.size() > 0) {
				mapping = XContentFactory.jsonBuilder().startObject().startObject(typename); // 必须是类型名称
				mapping.startObject("properties");
				for (int i = 0; i < newProList.size(); i++) {
					ESIndexProperty indexProperty = newProList.get(i);
					String name = indexProperty.getName();
					String index = indexProperty.getIndex();
					String type = indexProperty.getType();
					mapping = mapping.startObject(name).field("type", type).field("index", index).endObject();
				}
				mapping = mapping.endObject().endObject().endObject();
			}
		} catch (Exception tr) {
			log.error("get mapping error, indexname=" + indexname + ",typename=" + typename, tr);
			return;
		}
		if (mapping == null) {
			log.info("mapping No change, indexname=" + indexname);
			return;
		}
		try {
			PutMappingRequest mappingRequest = Requests.putMappingRequest(indexname).type(typename).source(mapping);
			indicesAdminClient.putMapping(mappingRequest).actionGet();
		} catch (Exception tr) {
			log.error("update index field error, indexname=" + indexname + ",typename=" + typename, tr);
			return;
		}
        log.info("updateIndexMapping indexname=" + indexname + " successfully!");
	}

    /**
     * 获取需要更新的字段
     *
     * @param indexname
     * @param typename
     * @param indexPropertyList
     * @return
     */
    public List<ESIndexProperty> getUpdateFieldList(String indexname, String typename, List<ESIndexProperty> indexPropertyList) {
        Map fieldMap = getPropertiesMapping(indexname, typename);
        if (fieldMap == null || fieldMap.isEmpty()) {
            return indexPropertyList;
        }

        List<ESIndexProperty> updatePropertyList = new ArrayList<ESIndexProperty>();
        for (int i = 0; i < indexPropertyList.size(); i++) {
            ESIndexProperty indexProperty = indexPropertyList.get(i);
            String name = indexProperty.getName();
            if (fieldMap.containsKey(name)) {
                continue;
            }
            updatePropertyList.add(indexProperty);
        }
        return updatePropertyList;
    }

    /**
     * 获取索引mapping映射
     *
     * @param indexname
     * @param typename
     * @return
     */
    public Map getPropertiesMapping(String indexname, String typename) {
        Map promap = null;
        Map<String, Object> sourceMap = getIndexSourceMap(indexname, typename);
        if (sourceMap == null) {
            return null;
        }
        promap = (Map) sourceMap.get("properties");
        return promap;
    }

    public Map<String, Object> getIndexSourceMap(String indexname, String typename) {
        if (indexname == null || typename == null) {
            return null;
        }
        Map<String, Object> sourceMap = null;
        try {
            if (indexname.equals("sigmam_fault")) {
                indexname = EsIndexDateUtil.getInstance().getEsDateTableName(indexname, System.currentTimeMillis());
            }
            TransportClient client = ESClientUtil.getInstance().getClient();
            ImmutableOpenMap<String, IndexMetaData> immutableOpenMap =
                    client.admin().cluster().prepareState().execute().actionGet().getState().getMetaData().getIndices();
            ImmutableOpenMap<String, MappingMetaData> mappings = immutableOpenMap.get(indexname).getMappings();
            if (mappings == null) {
                log.info("mappings is null, indexname=" + indexname);
                return null;
            }
            MappingMetaData mappingMetaData = mappings.get(typename);
            if (mappingMetaData == null) {
                log.info("mappingMetaData is null, indexname=" + indexname + ",typename=" + typename);
                return null;
            }
            sourceMap = mappingMetaData.getSourceAsMap();
        } catch (Exception e) {
            log.error("getIndexMapping error,indexname=" + indexname + ",typename=" + typename, e);
        }
        return sourceMap;
    }

    /**
     * 创建资源索引mapping
     *
     * @param indexname 索引名
     */
    public void createResourceIndexMapping(String indexname) {
        List<ESIndexProperty> indexPropertyList = ESResourceIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get resource indexPropertyList");
            return;
        }

        this.createIndexMapping(indexname, ESConstans.INDEX_TYPE, indexPropertyList);
    }

    /**
     * 创建索引(如果存在索引默认不更新)
     *
     * @param indexname
     * @param typename
     * @param indexPropertyList
     */
    public void createIndexMapping(String indexname, String typename, List<ESIndexProperty> indexPropertyList) {
        createIndexMapping(indexname, typename, indexPropertyList, false, indexname);
    }

    /**
     * 创建告警索引mapping
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createAllFaultIndexMapping(String appId, boolean needIndexUpdate) {
        this.createOrgeventIndexMapping(appId, false);
        this.createEventIndexMapping(appId, false);
        this.createHistoryFaultIndexMapping(appId, false);
        this.createCardFaultIndexMapping(appId, false);
        this.createNotifyFaultIndexMapping(appId, false);
        this.createRepeatNotifyFaultIndexMapping(appId, false);
        this.createItemFaultIndexMapping(appId, false);
        this.createUpgradeFaultIndexMapping(appId, false);
        this.createAlarmShieldRuleFaultIndexMapping(appId, false);
        this.createAlarmShieldRuleNotifyIndexMapping(appId, false);
        this.createTrapIndexMapping(appId, false);
        this.createThirdIndexMapping(appId, false);
    }

    /**
     * 创建原始事件索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createOrgeventIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmOrgEventIndex(appId);
        List<ESIndexProperty> indexPropertyList = OrgeventFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get orgevent indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建trap原始数据索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createTrapIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmTrapIndex(appId);
        List<ESIndexProperty> indexPropertyList = TrapIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get trap indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建第三方接入原始数据索引
     */
    public void createThirdIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmThirdIndex(appId);
        List<ESIndexProperty> indexPropertyList = ThirdIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get third indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建事件索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createEventIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmEventIndex(appId);
        List<ESIndexProperty> indexPropertyList = EventFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get event indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建历史告警索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createHistoryFaultIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmHistoryAlertIndex(appId);
        List<ESIndexProperty> indexPropertyList = HistoryFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get history alert indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建告警卡片索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createCardFaultIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmAlertCardIndex(appId);
        List<ESIndexProperty> indexPropertyList = CardFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get card alert indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建告警通知索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createNotifyFaultIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmNotifyRecordIndex(appId);
        List<ESIndexProperty> indexPropertyList = NotifyFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get alert notify record indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }


    /**
     * 创建告警重复通知索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createRepeatNotifyFaultIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmRepeatNotifyRecordIndex(appId);
        List<ESIndexProperty> indexPropertyList = RepeatNotifyFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get alert repeatNotify record indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建工单记录索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createItemFaultIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmItemRecordIndex(appId);
        List<ESIndexProperty> indexPropertyList = ItemFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get alert item record indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建告警升级记录索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createUpgradeFaultIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmUpgradeRecordIndex(appId);
        List<ESIndexProperty> indexPropertyList = UpgradeFaultIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get alert upgrade record indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建告警屏蔽规则（工程告警）规则记录索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createAlarmShieldRuleFaultIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmAlarmShieldRuleIndex(appId);
        List<ESIndexProperty> indexPropertyList = AlarmShieldRuleIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get alert upgrade record indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建屏蔽规则通知记录索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createAlarmShieldRuleNotifyIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getFmAlarmShieldRuleNotifyRecordIndex(appId);
        List<ESIndexProperty> indexPropertyList = AlarmShieldRuleNotifyIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get alert upgrade record indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 创建原始数据索引
     *
     * @param appId
     * @param needIndexUpdate
     */
    public void createRawDataIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getRawDataIndex(appId);
        List<ESIndexProperty> indexPropertyList = RawDataIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get raw data record indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }

    /**
     * 更新index.max_result_window
     *
     * @param indexname
     * @param maxResultWindow
     */
    public void updateIndexMaxResultWindow(String indexname, int maxResultWindow) {
        try {
            TransportClient client = ESClientUtil.getInstance().getClient();
            boolean isexists = client.admin().indices().prepareExists(indexname).get().isExists();
            if (isexists) {
                Settings settings = Settings.builder().put("index.max_result_window", maxResultWindow).build();
                ESClientUtil.getInstance().getClient().admin().indices().prepareUpdateSettings(indexname).setSettings(settings).execute().actionGet();
            }
        } catch (Exception e) {
            log.error("updateIndexMaxResultWindow error,indexname=" + indexname + ",maxResultWindow=" + maxResultWindow, e);
        }
    }

    public boolean validPmMappingType(String indexname, String typename) {
        boolean result = true;
        Map map = getPropertiesMapping(indexname, typename);
        if (map != null) {
            boolean kbpFlag = validFieldType(map, "KBP", "long");
            boolean kpiFlag = validFieldType(map, "KPI_NO", "long");

            if (!kbpFlag || !kpiFlag) {
                result = false;
            }
        } else {
            log.warn("map is null,indexname=" + indexname + ", typename=" + typename);
        }

        return result;
    }

    /**
     * 验证字段类型
     *
     * @param map
     * @param field
     * @param value
     * @return
     */
    private boolean validFieldType(Map map, String field, String value) {
        boolean result = false;
        try {
            Map fmap = (Map) map.get(field);
            if (fmap != null) {
                String type = (String) fmap.get("type");
                if (type != null && type.equalsIgnoreCase(value)) {
                    result = true;
                }
            }
        } catch (Exception e) {
            log.error("validFieldType error,map=" + map + ",field=" + field + ",value=" + value, e);
        }
        return result;
    }

    /**
     * 创建操作记录索引mapping
     *
     * @param appId
     */
    public void createOptRecordIndexMapping(String appId, boolean isupate) {
        List<ESIndexProperty> indexPropertyList = OperateRecordIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get resource indexPropertyList");
            return;
        }

        String indexAlias = ESConstans.getOperateRecordIndex(appId);
        String curIndexName = indexAlias + "_000000";
        createIndexMapping(curIndexName, ESConstans.INDEX_TYPE, indexPropertyList, isupate, indexAlias);
    }

    /**
     * 创建异常告警索引mapping
     *
     * @param appId
     */
    public void createCollectAbnormalAlarmIndexMapping(String appId, boolean isupate) {
        List<ESIndexProperty> indexPropertyList = AbnormalAlarmIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get abnormal alarm indexPropertyList");
            return;
        }

        String indexAlias = ESConstans.getCollectAbnormalAlarmIndex(appId);
        String curIndexName = indexAlias + "_000000";
        createIndexMapping(curIndexName, ESConstans.INDEX_TYPE, indexPropertyList, isupate, indexAlias);
    }


    public void updateResourceIndexMapping(String indexname) {
        List<ESIndexProperty> indexPropertyList = ESResourceIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get resource indexPropertyList");
            return;
        }
        createIndexMapping(indexname, ESConstans.INDEX_TYPE, indexPropertyList, true, indexname);
    }

    /**
     * 创建异常告警索引mapping
     *
     * @param appId
     */
    public void createProcessTraceMapping(String appId, boolean isupate) {
        List<ESIndexProperty> indexPropertyList = ProcessTraceIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get process trace indexPropertyList");
            return;
        }

        String indexAlias = ESConstans.getpProcessTraceIndex(appId);
        String curIndexName = indexAlias + "_000000";
        createIndexMapping(curIndexName, ESConstans.INDEX_TYPE, indexPropertyList, isupate, indexAlias);
    }

    /**
     * 删除异常告警索引mapping
     * @param appId
     */
	public void deleteProcessTraceMapping(String appId) {
		String indexAlias = ESConstans.getpProcessTraceIndex(appId);
		String curIndexName = indexAlias + "_000000";
		try {
			TransportClient client = ESClientUtil.getInstance().getClient();
			IndicesAdminClient indicesAdminClient = client.admin().indices();
			AcknowledgedResponse acknowledgedResponse = indicesAdminClient.prepareDelete(curIndexName).execute().actionGet();
            if (acknowledgedResponse.isAcknowledged()) {
				log.info("delete index " + curIndexName + " successfully");
			} else {
				log.warn("delete index " + curIndexName + " fail");
			}
		} catch (Exception e) {
			log.error("delete index error, index=" + curIndexName, e);
		}
	}

    public void createAnalyzeLogIndexMapping(String appId, boolean needIndexUpdate) {
        String indexAlias = ESConstans.getRawAnalyzeLogIndex(appId);
        List<ESIndexProperty> indexPropertyList = AnalyzeLogIndexPropertyXmlConfig.getInstance().getIndexPropertyList();
        if (indexPropertyList == null || indexPropertyList.size() <= 0) {
            log.error("can't get analyze log indexPropertyList");
            return;
        }
        priorCreateIndexesMapping(indexAlias, indexPropertyList, needIndexUpdate);
    }
}

