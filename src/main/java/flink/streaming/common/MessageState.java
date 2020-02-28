/*
 * $Id: MessageState.java,v 1.2 2008/07/22 05:32:28 wangqf Exp $
 */
package flink.streaming.common;

import java.io.Serializable;


/**
 * 消息状态类，包含所支持的Topic消息
 * @author  GuoQing Li
 * $Id: MessageState.java,v 1.2 2008/07/22 05:32:28 wangqf Exp $
 */
public class MessageState implements Serializable {
	//将要处理
    public static String PENDING = "pending";
    //正在处理
    public static String PROCESSING = "processing";
    //已经处理
    public static String PROCESSED = "processed";
    
    
    /**** ################ topic ################***/
    public static String CALLBACK_TOPIC = "qCallback";
    public static String NOTIFY_TOPIC = "qNotify";
    //发现
    public static String DISC_TOPIC = "qDisc";
    //采集
    public static String COLLECTSCHED_TOPIC = "qCollectSched";
    //拓扑
    public static String TOPO_TOPIC = "qTopo";
    //地图
    public static String MAP_TOPIC = "qMap";
    //配置
    public static String CONFIG_TOPIC = "qConfig";
    //性能
    public static String PERFORM_TOPIC = "qPerform";
    //事件
    public static String EVENT_TOPIC = "qEvent";
    //日志
    public static String LOG_TOPIC = "qLog";
    //轮询
    public static String STATUSPOLL_TOPIC = "qStatusPoll";
    //安全
    public static String SECURITY_TOPIC = "qSecurity";
    
    // Kafka测试topic
    public static String KAFKATEST_TOPIC = "qKafkaTest";
    
    // Storm处理测试事件topic (如：通过发测试事件， 监测storm组件处理流程是否顺畅 )
    public static String STORM_PROCESS_TESTEVENT_TOPIC = "qStormTestEvent";
    

    public static final String EXCHANGEDATA_TOPIC = "qExchangeData";
    
    /** syslog原始事件queue */
    public static final String QUEUE_ORGEVENT_SYSLOG="qOrgSyslog";
    
    /** trap原始事件queue */
    public static final String QUEUE_ORGEVENT_TRAP="qOrgTrap";
    
    /** poll原始事件queue */
    public static final String QUEUE_ORGEVENT_POLL="qOrgPoll";
    
    /** extevent原始事件queue */
    public static final String QUEUE_ORGEVENT_EXTEVENT="qOrgExtEvent";
    
    /** 其它事件上传queue */
    public static final String QUEUE_OTHER_EVENT="qOhterEvent";
    
    /** 原始业务数据消息上传queue    */
    public static final String QUEUE_RAW_BIZDATA="qRawBizData";
    
    /** 业务数据消息上传queue--cm     */
    public static final String QUEUE_BIZDATA_CM="qBizDataCM";
    
    /** 业务数据消息上传queue--pm     */
    public static final String QUEUE_BIZDATA_PM="qBizDataPM";
    
    /** 业务数据消息上传queue--fm     */
    public static final String QUEUE_BIZDATA_FM="qBizDataFM";
    
    /** 动态基线样本空间变化     */
    public static final String QUEUE_BASELINE="qBaseline";
    
    /** 在storm中处理完成，转发到消息服务器的数据     */
    public static final String QUEUE_PM_TRANSFE_DATA="qPMTransfeData";
    
    
    /** 在storm中处理，SNMP原始数据    */
    public static final String QUEUE_SNMP_ORIGIN_DATA="qSnmpOriginData";
    
    /** 在storm中处理异常 （如：storm组件所在的服务器资源不足，丢失数据，发出异常信息）**/
    public static String STORM_PROCESS_ERROR_TOPIC = "qStromProcessError";
    
    
    
}
