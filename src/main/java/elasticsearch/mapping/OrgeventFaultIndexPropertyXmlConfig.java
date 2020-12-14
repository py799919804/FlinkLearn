package elasticsearch.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ultrapower.fsms.common.elasticsearch.ESMappingManager;
import com.ultrapower.fsms.common.msg.MessageClient;
import com.ultrapower.fsms.common.msg.MessageHandler;
import com.ultrapower.fsms.common.msg.util.Message;
import com.ultrapower.fsms.common.msg.util.MessageState;
import com.ultrapower.fsms.common.msg.util.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 原始事件mapping文件加载
 * @ClassName: OrgeventFaultIndexPropertyXmlConfig
 * @Description: TODO
 * @author lijianye
 * @date 2018年4月28日 下午1:29:50
 *
 */
public class OrgeventFaultIndexPropertyXmlConfig extends IndexPropertyXmlConfig {
	// 日志
	private static Logger log = LoggerFactory.getLogger(OrgeventFaultIndexPropertyXmlConfig.class);

	public static final String CONFIG_FILE = "es_fm_orgevent_mapping.xml";

	private static OrgeventFaultIndexPropertyXmlConfig instance = null;

	public static OrgeventFaultIndexPropertyXmlConfig getInstance() {
		if (instance == null) {
			synchronized (OrgeventFaultIndexPropertyXmlConfig.class) {
				if (instance == null) {
					instance = new OrgeventFaultIndexPropertyXmlConfig();
				}
			}
		}
		return instance;
	}

	private OrgeventFaultIndexPropertyXmlConfig() {
		loadConfig(CONFIG_FILE);
		startMessageListener();
	}

	/**
	 * 接收索引重新创建装载的消息
	 */
	private void startMessageListener() {
		log.info("Start message listener, topic=MessageState.CONFIG_TOPIC");
		try {
			MessageClient.getInstance().addMessageHandler(MessageState.CONFIG_RULE_TOPIC, new MessageHandler() {
				@Override
        public void onMessage(String msg) {
					Message msgObject = null;
					try {
						if(msg == null && "".equals(msg)){
							return;
						}else{
							ObjectMapper objectMapper = new ObjectMapper();
							msgObject = objectMapper.readValue(msg, Message.class);
						}
						if (msgObject.getType() == MessageType.ES_INDEX_FIELD_UPDATE) {
							Object obj = msgObject.getUserObject();
							if (ESMappingManager.ESMAPPING_FM.equalsIgnoreCase(obj + "")) {
								log.info("Start loadConfig : " + CONFIG_FILE);
								loadConfig(CONFIG_FILE);
							}
						}
					} catch (JsonProcessingException e) {
						e.printStackTrace();
					}
				}
			});
		} catch (Exception e) {
			log.error("Start message listener error,topic=MessageState.PERFORM_TOPIC", e);
		}

	}

}
