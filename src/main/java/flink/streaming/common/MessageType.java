/**
 * $Id: MessageType.java,v 1.52 2013/08/27 08:06:53 qiuyg Exp $
 *
 */
package flink.streaming.common;

import java.io.Serializable;

/**
 * 消息类型类
 * @author GuoQing Li
 * @version $Id: MessageType.java,v 1.52 2013/08/27 08:06:53 qiuyg Exp $
 */
public class MessageType implements Serializable {

    /**
     * Put Discovery Message Type here
     * Range:300001－310000
     * MessageState: MessageState.Disc
     */
    public static final long DISC_NODE = 300001L;
    public static final long DISC_NETWORK = 300002L;
    public static final long START_PROBE_DISC = 300003L;
    public static final long STOP_PROBE_DISC = 300004L;
    public static final long START_DISC = 300005L;
    public static final long STOP_DISC = 300006L;
    public static final long DISC_RECONFIG_TARGET_SNMPINFO = 300007L;
    public static final long DISC_REDISC_CONFIG = 300008L;
    public static final long PROBE_COMMUNITY_CHANGE= 300009L;    
    public static final long DISC_TEST_MSG=300010L;
    public static final long START_DISC_TEST_MSG=300011L;
    public static final long WEB_REALDATA_SEND=300012L;
    public static final long DISC_RES_ADD=300013L;
    public static final long DISC_RES_UPDATE=300014L;
    public static final long DISC_NODE_FINISH=300015L;
    public static final long REDISC_FINISH_MSG=300016L;
    public static final long DISC_LINK_COMPLETED=300017L;
    public static final long DISC_LINK_NODEDISC_COMPLETED=300018L;
    public static final long DISC_LINK=300019L;
    public static final long START_DISC_TEST_SSH_MSG=300020L;
    public static final long DISC_TEST_SSH_MSG=300021L;
    
    public static final long PROBE_RES_DELE=300022L;


    
    /**
     * Put Collect Schedule Message Type here
     * Range:310001－320000
     * MessageState: MessageState.CollectSched
     */
    public static final long DISPATCH_TASK = 310001L;
    public static final long PROBE_HEARTBEAT = 310002L;
    public static final long NETFLOW_UPLOAD = 310003L;
    public static final long SHUTDOWN_PROBE = 310004L;
    public static final long SHUTDOWN_ALLREALTIMECOLLECT_PROBE = 310005L;
    public static final long FRAME_HEARTBEAT = 310006L;
    public static final long MESSAGE_SEND = 310007L;
    public static final long BEGIN_NETFLOW_MONITOR = 310011L;
    public static final long END_NETFLOW_MONITOR = 310012L;
     public static final long UPDATE_HOSTGROUP = 310015L;
     public static final long UPDATE_COLLECTPARAM = 310016L;
     
     public static final long STOP_PROBE = 310021L;
     public static final long RESTART_PROBE= 310022L;
     public static final long START_PROBE= 310023L;
     public static final long PROBE_WATCHBEAT= 310024L;
     public static final long CUTOVER_SERVER= 310025L;
     public static final long MASTER_SERVER_START= 310026L;     
     public static final long PROBE_CHANGE_MSG = 310027L;  
     public static final long PROBE_PASSIVE_TASK= 310028L;  
     

     
     public static final long PROCESS_ADD= 310029L;     
     public static final long PROCESS_DEL= 310030L; 
     
     public static final long DEVICE_COLL_INDEX_CHANGE= 310031L;  
     /***
      * patrol 监控指标变更
      */
     public static final long PATROL_KPI_CHANGE= 310040L; 
     
    /**
     * Put Status Poll Type here
     * Range:320001－330000
     * MessageState: MessageState.StatusPoll
     */
    public static final long UPDATE_POLL_PARAMS = 320001L;
    public static final long POLL_MO = 320002L;
    public static final long START_POLL = 320003L;
    public static final long STOP_POLL = 320004L;
    public static final long START_PROBE_POLL = 320005L;
    public static final long STOP_PROBE_POLL = 320006L;
    public static final long START_POLL_MO = 320007L;
    public static final long STOP_POLL_MO = 320008L;
    public static final long POLL_REBUILDIPMACPORT = 320009L;


    /**
     * Put Topo Message Type here
     * Range:330001－340000
     * MessageState: MessageState.Topo
     */
    public static final long MO_ADDED = 330001L;
    public static final long MO_DELETED = 330002L;
    public static final long MO_MANAGED_UPDATE = 330003L;
    public static final long MO_STATUS_UPDATE = 330004L;
    public static final long MO_PROPERTY_UPDATE = 330005L;
     /*** 拓扑刷新消息*/
    public static final long TOPO_REFRESH = 330006L;
    /**Router 的 OSPF 信息*/
    public static final long MAP_INFO = 330007L;
    /**mo类型改变*/
    public static final long MO_CLASSNAME_UPDATE = 330008L;
   

    /**
     * Put Configuration Message Type here
     * Range:340001－350000
     * MessageState: MessageState.Config
     */
    public static final long CONFIG_TEST = 340001L;

    public static final long RESOBJECT_INSERT = 340011L;
    public static final long RESOBJECT_UPDATE = 340012L;
    public static final long RESOBJECT_DELETE = 340013L;
    public static final long RESCLASSDEF_CHANGE = 340014L;//类定义修改
    public static final long RESPHASE_CHANGE = 340015L;//类定义修改
    public static final long RESCLASSDEF_SYN = 340016L;//类定义同步
    public static final long RESOBJECT_EHCACHE_UPDATE = 340017L; // 资源ehcache缓存更新
    public static final long MULTISELECT_INSERT = 340018L; // 
    public static final long MULTISELECT_DELETE = 340019L; // 
    public static final long RES_REDIS_SYNC = 340020L; // redis同步
    public static final long ES_INDEX_FIELD_UPDATE = 340021L; // es索引字段变更
    
    public static final long RESCHEDULE_UPDATE = 341011L;
    public static final long RESMULTISELECT_INSERT = 341012L;
    
    public static final long RESNODE_DISC_FINISH = 351012L;
    
    /**用户登陆,退出消息*/
    public static final long USER_LOGIN_MSG=500002L;
  
    //trace返回消息
    public static final long REMOTE_TRACE_MSG=341001L;
    //ping返回消息
    public static final long REMOTE_PING_MSG=341002L;
    
    public static final long KPI_RELAOD_MSG=342001L;
    
    
    /*业务影响**/
    public static final long RESRELATE_STATUS_PROPAGATE=342001L;
    //资源对象健康状态变化
    public static final long MAP_RESEXTOBJECT_STATUS_UPDATE = 342002L;
    //资源对象告警状态变化
    public static final long MAP_RESEXTOBJECT_ALERTSTATUS_UPDATE = 342003L;
   
    /**SysName改变*/
    public static final long MO_SYSNAME_UPDATE = 342009L;
    /**IP位置改变，父节点或父对象改变*/
    public static final long MO_IP_UPDATE = 342010L;
    
    /**节点IP地址改变*/
    public static final long NODE_IP_UPDATE = 342011L;
    /**
     *  Definition of Map Message
     *  Range:350001-360000
     *  MessageState: MessageState.EVENT_TOPIC
     */
    public static final long MAP_ADDED = 350001L;
    public static final long MAP_DELETED = 350002L;
    public static final long MAP_UPDATE = 350003L;
    public static final long MAP_STATUS_UPDATE = 350004L;
    public static final long MAP_BATCH_UPDATE = 350005L;
    public static final long MAP_POSITION_UPDATE = 350008L;
    public static final long MAP_FIRST_LOADED=350006L;//WEBTOPO加载的时候，向后NmsServer发送此消息，NmsServer收到此消息后，向拓朴传递一次性能数据。
    public static final long MAP_LINKSTATUS_UPDATE = 350007L;
    public static final long MAPTYPEALERTRELATION_ADD=350010L;
    public static final long MAPTYPEALERTRELATION_DELETE=350011L;
    public static final long MAPTYPEALERTRELATION_REPLACE=350012L;
    //告警传递的消息
    public static final long MAP_STATUSPROPAGATETASK=350013L;
    //重算全部相关mo和图标的状态
    public static final long MAP_STATUSPROPAGATETASK_UPDATEALL=350014L;
    
    public static final long MAPLINK_ADD_BY3PARTY=350016L;
    public static final long MAPLINK_DELETE_BY3PARTY=350017L;
    
    
    //新资源对象的关系及状态改变(351001-352000)
    public static final long MAP_RESEXTOBJECT_MAINSTATUS_UPDATE = 351001L;
    public static final long MAP_RESEXTOBJECT_SUBSTATUS_UPDATE = 351002L;
   
    
    public static final long MAP_RELATIONSHIP_ADD = 351011L;
    public static final long MAP_RELATIONSHIP_DELETE = 351012L;
    public static final long MAP_RELATIONSHIP_UPDATE = 351013L;
    //业务拓扑图消息(352001-353000)
    public static final long BUSINESSMAP_ADDED = 352001L;
    public static final long BUSINESSMAP_DELETED = 352002L;
    public static final long BUSINESSMAP_UPDATE = 352003L;
    public static final long BUSINESSMAP_UPDATE_POSITION = 352004L;
    public static final long BUSINESSMAP_BATCH_UPDATE = 352005L;
    public static final long BUSINESSMAP_STATUS_UPDATE = 352006L;
    public static final long BUSINESSMAPLINK_ADDED = 352101L;
    public static final long BUSINESSMAPLINK_DELETED = 352102L;
    public static final long BUSINESSMAPLINK_UPDATE = 352103L;
    public static final long BUSINESSMAPSYMBOL_ADDED = 352201L;
    public static final long BUSINESSMAPSYMBOL_DELETED = 352202L;
    public static final long BUSINESSMAPSYMBOL_UPDATE = 352203L;
    public static final long BUSINESSMAPSYMBOL_UPDATE_POSITION = 352204L;
    public static final long BUSINESSMAPGROUP_ADDED = 352301L;
    public static final long BUSINESSMAPGROUP_DELETED = 352302L;
    public static final long BUSINESSMAPGROUP_UPDATE = 352303L;
    public static final long HEALTHSTATUSINTERVALRANGES_DEFS_UPDATE = 352401L;
    public static final long IMACTLEVEL_DEFS_UPDATE = 352402L;
    //深登项目自动定位业务视图节点的消息
    public static final long BUSINESS_AUTO_LOCATE = 360000L;
    /**拓扑图标*/
    public static final long MAP_SYMBOL_ICONSTATS = 361000L;
    
    //路由监测消息(3551100-3551200)
    public static final long ROUTEMONITOR_TRACEROUTE_BEGIN = 3551100L;
    public static final long ROUTEMONITOR_TRACEROUTE_FINISH = 3551101L;
    public static final long ROUTEMONITOR_TRACEROUTE_MOREPATH = 3551102L;
    public static final long ROUTEMONITOR_TRACEROUTE_STOP = 3551103L;
    
    //wlan远程升级计划消息(3551201-3551300)
    public static final long WLAN_UPDATESCHEDULE_ADD = 3551201L;
    public static final long WLAN_UPDATESCHEDULE_DELETE = 3551202L;
    public static final long WLAN_UPDATESCHEDULE_UPDATE = 3551203L;

    /**
     *  Definition of Fault Message
     *  Range:370001-380000
     *  MessageState: MessageState.EVENT_TOPIC
     */
    public static final long EVENT_MSG         = 370001L;
    public static final long UPDATEFILTERMSG   = 370002L;
    public static final long UI_EVENT_MSG         = 370003L;
    public static final long UI_ALERT_MSG         = 370004L;
    public static final long UI_SYSJOB_MSG         = 370005L;
    public static final long CLEAR_ALERT_MSG         = 370006L;
    public static final long HURRAY_LIMIT_MSG         = 370007L;
    public static final long SYN_EVENT_MSG         = 370008L;
    public static final long UPDATE_CATEGORY_MSG         = 370009L;
    public static final long FAULT_ADD_ADD = 370010L;
    public static final long UPGRADE_ALERT_MSG		=370011L;
    public static final long FAULT_ADD_ITEM		=370012L;
    public static final long FAULT_UPDATE_SCHEME     =370013L;//更新事件相关性分析策略消息类型
    public static final long FAULT_ITEM_DISPATCHSUCCESS     =370014L;//更新事件相关性分析策略消息类型
    public static final long ACK_ALERT_MSG     =370015L;//告警确认消息类型
    public static final long FAULT_SEVERTIY_CHANGE =370016L;// 告警级别发生改变消息类型
    public static final long FAULT_SOUND_NOTIFY = 370017L;     // 告警声音通知消息类型
    public static final long FAULT_QUERY_VIEW_UPDATE = 370018L; //故障查询视图更新消息类型
    public static final long FAULT_MANUAL_STOP_DISPATCH_ITEM = 370019L; //人工停止派单消息
    public static final long FAULT_EVENT_SUPPRESS_SCHEME_UPDATE = 370020L; //事件压制策略
    public static final long DB_CONFIG_CHANGED = 370021L; //数据库连接配置发生变化
    public static final long FAULT_UPDATE_ORGEVENTFILTER_RULE = 370022L;//原始事件过滤规则消息更新
    public static final long PROJECT_MARK_MSG     =370023L;//告警工程标识消息类型
    public static final long ALERT_UPDATE     =370024L;//告警工程标识消息类型
    public static final long FAULT_EC_ALERT_UPDATE     =370025L;//相关性分析结束更新告警
    public static final long FAULT_SOUND_NOTIFY_TO_WEB = 370026L;     // 发送声音通知到Web端
    public static final long FAULT_ITEM_FAILED     = 370027L;//派单失败消息
    public static final long FAULT_ITEM_DISPATCHING     = 370028L;//正在派单中消息
    public static final long FAULT_NOTIFY_MANUAL_SEND     = 370029L;//手动派发通知消息
    public static final long FAULT_ITEM_STATUS_CLEAR_UPDATE     = 370030L;//工单状态同步再次发清除消息
    public static final long FAULT_REPEAT_SOUND_NOTIFY_TO_WEB = 370031L;     // 发送重复声音通知到Web端
    public static final long FAULT_NOTIFY_SENDER_MAIL = 370032L;     // 默认的邮件接口消息发送类型
    public static final long FAULT_NOTIFY_SENDER_SMS = 370033L;     //  默认的短信接口消息发送类型
    public static final long FAULT_NOTIFY_SENDER_EXEC = 370034L;     // 默认的EXEC接口消息发送类型
    public static final long FAULT_NOTIFY_BY_RESOURCE = 370035L;     // 以人为中心的告警通知规则变更消息    
    public static final long FAULT_PARENT_MAP_CACULATE = 370036L;     // 父拓朴状态计算消息类型
    
    public static final long FAULT_RECEIVE_MSG = 370040L; // 告警接收到消息
    /**
     *  Definition of Performance Message
     *  Range:380001-390000
     *  MessageState: MessageState.EVENT_TOPIC
     */
    public static final long PERFORMANCE_REALTIME_MSG         = 380001L;
    public static final long PERFORMANCE_UPGRADE_THRESHOLD    = 380002L;
    public static final long PERFORMANCE_UPGRADE_CUSTKPI    = 380003L;
    public static final long PERFORMANCE_OID2KPI_UPDATE    = 380004L;
    public static final long PERFORMANCE_KPI_UPDATE    = 380005L;   //KPI指标加载完成
    public static final long PERFORMANCE_UPGRADE_THRESHOLD_FINISHED    = 380006L; //门限规则更新完成
 
    /**
     * Put service monitor message Type here
     * Range:390001－400000
     * MessageState: MessageState.StatusPoll
     */
    /**更改一个服务的monitor参数*/
    public static final long UPDATE_MONITOR_PARAMS = 390001L;
    /**服务监控仅一次，提供 测试接口*/
    public static final long MONITOR_SERVICE_ONCE = 390002L;
    /**启动单个服务监控*/
    public static final long START_ONE_MONITOR = 390003L;
    /**停止单个服务监控*/
    public static final long STOP_ONE_MONITOR = 390004L;
    /**启动某个Probe的服务监控*/
    public static final long START_RROBE_MONITOR = 390005L;
    /**停止某个Probe服务的监控*/
    public static final long STOP_PROBE_MONITOR = 390006L;
    /**启动所有服务的监控*/
    public static final long START_MONITOR = 390007L;
    /**停止所有服务的监控*/
    public static final  long STOP_MONITOR = 390008L;
    /**发送监控结果信息*/
    public static final  long SEND_MONITORINFO = 390009L;
    
    public static final long MONITOR_CREATE = 3900011L;
    
    public static final long MONITOR_REMOVE = 3900012L;

    /**
     * Put Performance message Type here
     * Range:400001－410000
     * MessageState: MessageState.PERFORM_TOPIC
     */
    //性能流量指标变化
    public static final long PERFORM_TRAFFICE_CHANGED = 400001L;
    
    public static final long IB_CLASS_ADDED    = 411001L;
    public static final long IB_CLASS_DELETED    = 411002L;
    public static final long IB_CLASS_UPDATED    = 411003L;
    public static final long IB_CLASS_RELOADED    = 411004L;
    
    /**
	 * Put unicomsms ExchangeData message Type here
	 * Range:600001－610009
	 * MessageState: MessageState.EXCHANGEDATA_TOPIC
	 * 
	 * message的addtional为3位的字符串，依次为DB-LINK接口、SNMP接口、tgpp接口。
	 * 只能为0或1，0表示此类接口不需要set或get，1表示需要set或get或在回执中表示已set、get
	 * 例如111，表示3类接口都需要处理。
	 * Message.getUserObject()，为一个List（DB接口需要set或get的KBP)
	 */
	/**设置SPCP上传标识 */
	public static final long SET_SPCP = 600001L;
	/** 表示接口已set*/
	public static final long SET_SPCP_BACK = 600002L;
	 /** 设置号段上传标识 */
	public static final long SET_MOBILE_NUM = 600003L;
	/** 表示接口已set*/
	public static final long SET_MOBILE_NUM_BACK = 600004L;

	/** 获取SPCP信息 */
	public static final long GET_SPCP = 610001L;
	/** 表示接口已set*/
	public static final long GET_SPCP_BACK = 610002L;
	/** 获取号段信息 */
	public static final long GET_MOBILE_NUM = 610003L;
	/** 表示接口已get*/
	public static final long GET_MOBILE_NUM_BACK = 610004L;	
	
	//probe执行show模板
	public static final long CONFIG_RUN_TMEPLATE = 620001L;
	//show模板执行后的返回信息
	public static final long CONFIG_RUN_CALLBACK = 620002L;
	
	//probe执行下载模板
	public static final long CONFIG_DOWNLOAD_TEMPLATE = 630001L;
	//schedule模块保存文件信息
	public static final long CONFIG_DOWNLOAD_SAVEFILE = 630002L;
	//听下载后的文件信息
	public static final long CONFIG_DOWNLOAD_CALLBACK = 630003L;
	
	//schedule模块读取上载文件信息
	public static final long CONFIG_UPLOAD_READFILE = 640001L;
	//执行上载模板
	public static final long CONFIG_UPLOAD_TEMPLATE = 640002L;
	//执行上载模板
	public static final long CONFIG_UPLOAD_CALLBACK = 640003L;
	
	//远程Traceroute
	public static final long CONFIG_REMOTE_TRACE = 650001L;
	//远程Traceping
	public static final long CONFIG_REMOTE_PING = 650002L;
	
	//保存文件
	public static final long CONFIG_FILE_SAVE = 660001L;
	//保存文件回调信息
	public static final long CONFIG_FILE_SAVEBACK = 660002L;
	//查看文件
	public static final long CONFIG_FILE_SHOW = 660003L;
	//查找文件回调信息
	public static final long CONFIG_FILE_SHOWBACK = 660004L;
	//删除文件
	public static final long CONFIG_FILE_REMOVE = 660005L;
	//删除文件回调信息
	public static final long CONFIG_FILE_REMOVEBACK = 660006L;
	//更新importResObjfilters.xml
	public static final long CONFIG_FILE_UPDATE_IMPORTRESOBJFILTERS= 660007L;
	
	
	
	
	/**
     *  Definition of Security Message
     *  Range:420001-430000
     *  MessageState: MessageState.SECURITY_TOPIC
     */
	public static final long DIMENSION_PERMISSION_CHANGED = 420001L;
	
    /**
     * Put Notify Message Type here
     * Range:500000-510000
     * MessageState: MessageState.NOTIFY_TOPIC
     */
	//System.properties属性变更消息
    public static final long NOTIFY_SYSTEM_PROPERTIES_CHANGE = 500000L;
    //Redis中配置文件变更消息
    public static final long NOTIFY_REDIS_CONFIG_FILE_CHANGE = 500001L;
    //zookeeper中全局系统参数配置
    public static final long NOTIFY_ZK_GLOBAL_CONFIG_CHANGE = 500003L;
    //增加probe组
    public static final long ADD_PROBE_GROUP = 500004L;
    //删除probe组
    public static final long DELETE_PROBE_GROUP = 500005L;
    //更新probe组
    public static final long UPDATE_PROBE_GROUP = 500006L;
    //数据库地址变更消息
    public static final long NOTIFY_DATABASE_ADDRESS_CHANGE = 500007L;
    
    //cmc自管理状态监控参数变更
    public static final long CMC_STATUSMONITOR_PARAM_CHANGE = 500008L;
    
    //probe公网ip变更
    public static final long PROBE_PUBLICNETIP_CHANGE = 500066L;
    
    
    //python脚本变更
    public static final long PYTHON_PLUGIN_CHANGE = 510001L;
    
    /**
     *  Definition of meta-test Message
     *  Range:620001－620100
     *  MessageState: MessageState.METATEST_TOPIC
     */
    public static final long METAQ_HEARTBEAT = 620001L;
    
    //基线消息类型
    /** 基线数据发生变化*/
    public static final long BASELINE_DATA_CHANGED = 700001L;
    
}
