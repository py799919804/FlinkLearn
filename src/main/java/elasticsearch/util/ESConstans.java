package elasticsearch.util;

/**
 * @Title:
 *
 * @description:
 * 
 * @Company: ultrapower.com
 * @author ljy
 * @create time：2016年5月30日 下午1:30:32
 * @version 1.0
 */
public class ESConstans {

	public interface FieldType {
		public static final String LONG = "long";
		public static final String INTEGER = "integer";
		//public static final String STRING = "string";
	}

	/**
	 * 索引类型（所有索引使用相同类型）
	 */
	public static final String INDEX_TYPE = "sigmac"; // 索引类型（所有索引使用相同类型）
	
	
	/**
	 * 原始事件索引
	 */
	public static final String FM_ORGEVENT_INDEX_SUFFIX = "_fm_orgevent"; // 原始事件索引
	/**
	 * trap原始数据表
	 */
	public static final String FM_TRAP_INDEX_SUFFIX = "_fm_rawdata_trap"; // trap原始数据表
	/**
	 * 第三方接入原始数据表
	 */
	public static final String FM_THIRD_INDEX_SUFFIX = "_fm_rawdata_thirdparty"; // trap原始数据表
	/**
	 * 事件索引
	 */
	public static final String FM_EVENT_INDEX_SUFFIX = "_fm_event"; // 事件索引
	/**
	 * 历史告警索引
	 */
	public static final String FM_HISTORYALERT_INDEX_SUFFIX = "_fm_historyalert"; // 历史告警索引
	/**
	 * 告警处理卡片(告警确认、清除记录)
	 */
	public static final String FM_ALERTCARD_INDEX_SUFFIX = "_fm_alertcard"; // 告警处理卡片(告警确认、清除记录)
	/**
	 * 告警通知记录 
	 */
	public static final String FM_NOTIFYRECORD_INDEX_SUFFIX = "_fm_notifyrecord"; // 告警通知记录
	/**
	 * 告警重复通知记录
	 */
	public static final String FM_REPEATNOTIFYRECORD_INDEX_SUFFIX = "_fm_repeatnotifyrecord"; // 告警重复通知记录
	/**
	 * 工单记录
	 */
	public static final String FM_ITEMRECORD_INDEX_SUFFIX = "_fm_itemrecord"; // 工单记录
	/**
	 * 告警升级记录
	 */
	public static final String FM_UPGRADERECORD_INDEX_SUFFIX = "_fm_upgraderecord"; // 告警升级记录
	/**
	 * 告警屏蔽规则
	 */
	public static final String FM_ALARMSHIELDRULE_INDEX_SUFFIX = "_fm_alarmshieldrule"; // 告警屏蔽规则
	/**
	 * 告警屏蔽规则通知记录
	 */
	public static final String FM_ALARMSHIELDRULE_NOTIFY_INDEX_SUFFIX = "_fm_alarmshieldrule_notifyrecord"; // 告警屏蔽规则


	/**
	 * 数据处理跟踪
	 */
	public static final String PROCESS_TRACE_SUFFIX = "_process_tracerecord";
	
	/**
	 * 采集终端异常告警
	 */
	public static final String COLLECT_ABNORMAL_ALARM_SUFFIX = "_abnormalalarm"; // 采集终端异常告警
	/**
	 * 操作记录
	 */
	public static final String OPERATE_RECORD_INDEX_SUFFIX = "_operaterecord"; // 操作记录日志
	

	/**
	 * 资源索引
	 */
	public static final String RESOURCE_INDEX_SUFFIX = "_resource"; // 资源索引
	
	
	/**
	 * 原始数据(PM / CM)
	 */
	public static final String RAW_DATA_INDEX_SUFFIX = "_rawdata"; // 原始数据

	/**
	 *
	 */
	public static final String RAW_ANALYZE_LOG_SUFFIX = "_raw_analyze_log";
	
	
	
	

	
	
	/**
	 * 资源索引名称
	 * 
	 * @param appId
	 * @return
	 */
	public static String getResourceIndex(String appId) {
		return (appId + ESConstans.RESOURCE_INDEX_SUFFIX).toLowerCase();
	}


	
	/**
	 * 原始事件索引名称
	 * 
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmOrgEventIndex(String appId) {
		return (appId + ESConstans.FM_ORGEVENT_INDEX_SUFFIX).toLowerCase();
	}

	/**
	 * trap原始数据索引名称
	 *
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmTrapIndex(String appId) {
		return (appId + ESConstans.FM_TRAP_INDEX_SUFFIX).toLowerCase();
	}

	/**
	 * 第三方接入原始数据索引名称
	 *
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmThirdIndex(String appId) {
		return (appId + ESConstans.FM_THIRD_INDEX_SUFFIX).toLowerCase();
	}

	/**
	 * 事件索引名称
	 * 
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmEventIndex(String appId) {
		return (appId + ESConstans.FM_EVENT_INDEX_SUFFIX).toLowerCase();
	}
	
	/**
	 * 历史告警索引名称
	 * 
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmHistoryAlertIndex(String appId) {
		return (appId + ESConstans.FM_HISTORYALERT_INDEX_SUFFIX).toLowerCase();
	}
	
	/**
	 * 告警处理卡片(确认、清除)索引名称
	 * 
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmAlertCardIndex(String appId) {
		return (appId + ESConstans.FM_ALERTCARD_INDEX_SUFFIX).toLowerCase();
	}
	
	/**
	 * 告警通知记录索引名称
	 * 
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmNotifyRecordIndex(String appId) {
		return (appId + ESConstans.FM_NOTIFYRECORD_INDEX_SUFFIX).toLowerCase();
	}

	/**
	 * 告警重复通知记录索引名称
	 *
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmRepeatNotifyRecordIndex(String appId) {
		return (appId + ESConstans.FM_REPEATNOTIFYRECORD_INDEX_SUFFIX).toLowerCase();
	}


	/**
	 * 告警派单记录索引名称
	 * 
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmItemRecordIndex(String appId) {
		return (appId + ESConstans.FM_ITEMRECORD_INDEX_SUFFIX).toLowerCase();
	}
	
	/**
	 * 告警升级级别索引名称
	 * @param appId
	 * @return
	 * @author caijinpeng
	 */
	public static String getFmUpgradeRecordIndex(String appId) {
		return (appId + ESConstans.FM_UPGRADERECORD_INDEX_SUFFIX).toLowerCase();
	}
	
	/**
	 * 告警屏蔽规则索引名称
	 * @param appId
	 * @return
	 */
	public static String getFmAlarmShieldRuleIndex(String appId) {
		return (appId + ESConstans.FM_ALARMSHIELDRULE_INDEX_SUFFIX).toLowerCase();
	}
	
	/**
	 * 告警屏蔽规则通知记录索引名称
	 * @param appId
	 * @return
	 */
	public static String getFmAlarmShieldRuleNotifyRecordIndex(String appId) {
		return (appId + ESConstans.FM_ALARMSHIELDRULE_NOTIFY_INDEX_SUFFIX).toLowerCase();
	}
	
	/**
	 * 原始数据
	 * @param appId
	 * @return
	 */
	public static String getRawDataIndex(String appId) {
		return (appId + ESConstans.RAW_DATA_INDEX_SUFFIX).toLowerCase();
	}
	
	/**
	 * 操作记录
	 * @param appId
	 * @return
	 */
	public static String getOperateRecordIndex(String appId) {
		return (appId + ESConstans.OPERATE_RECORD_INDEX_SUFFIX).toLowerCase();
	}
	
	
	/**
	 * 采集终端异常告警
	 * @param appId
	 * @return
	 */
	public static String getCollectAbnormalAlarmIndex(String appId) {
		return (appId + ESConstans.COLLECT_ABNORMAL_ALARM_SUFFIX).toLowerCase();
	}

	/**
	 * 数据处理跟踪
	 * @param appId
	 * @return
	 */
	public static String getpProcessTraceIndex(String appId) {
		return (appId + ESConstans.PROCESS_TRACE_SUFFIX).toLowerCase();
	}

	/**
	 * 原始分析日志
	 * @param appId
	 * @return
	 */
	public static String getRawAnalyzeLogIndex(String appId) {
		return (appId + ESConstans.RAW_ANALYZE_LOG_SUFFIX).toLowerCase();
	}
}
