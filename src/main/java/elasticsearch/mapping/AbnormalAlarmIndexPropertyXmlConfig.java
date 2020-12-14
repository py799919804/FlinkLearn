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
 * 
 * @ClassName:     AbnormalAlarmIndexPropertyXmlConfig
 * @Description:   采集终端异常告警记录索引
 * 
 * @company        北京神州泰岳软件股份有限公司
 * @author         caijinpeng
 * @email          caijinpeng@ultrapower.com.cn
 * @version        V1.0
 * @Date           2018年9月26日 下午4:46:27
 */
public class AbnormalAlarmIndexPropertyXmlConfig extends IndexPropertyXmlConfig{
	// 日志
	private static Logger log = LoggerFactory.getLogger(AbnormalAlarmIndexPropertyXmlConfig.class);

	public static final String CONFIG_FILE = "es_abnormal_alarm_mapping.xml";

	private static AbnormalAlarmIndexPropertyXmlConfig instance = null;

	public static AbnormalAlarmIndexPropertyXmlConfig getInstance() {
		if (instance == null) {
			synchronized (AbnormalAlarmIndexPropertyXmlConfig.class) {
				if (instance == null) {
					instance = new AbnormalAlarmIndexPropertyXmlConfig();
				}
			}
		}
		return instance;
	}

	private AbnormalAlarmIndexPropertyXmlConfig() {
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
							if (ESMappingManager.ESMAPPING_ABNORMAL.equalsIgnoreCase(obj + "")) {
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
