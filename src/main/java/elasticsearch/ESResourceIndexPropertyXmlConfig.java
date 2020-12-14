package elasticsearch;//package com.ultrapower.fsms.common.elasticsearch;
//
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.ultrapower.fsms.common.dubbo.RMIUtil;
//import com.ultrapower.fsms.common.msg.MessageClient;
//import com.ultrapower.fsms.common.msg.MessageHandler;
//import com.ultrapower.fsms.common.msg.util.Message;
//import com.ultrapower.fsms.common.msg.util.MessageState;
//import com.ultrapower.fsms.common.msg.util.MessageType;
//import com.ultrapower.fsms.common.utils.CommonIOUtils;
//import org.dom4j.Document;
//import org.dom4j.Element;
//import org.dom4j.io.SAXReader;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.ByteArrayInputStream;
//import java.io.InputStream;
//import java.io.InputStreamReader;
//import java.io.Reader;
//import java.util.*;
//
///**
// * @Title:
// *
// * @description:
// *
// * @Company: ultrapower.com
// * @author ljy
// * @create time：2016年5月16日  上午10:13:37
// * @version 1.0
// */
//public class ESResourceIndexPropertyXmlConfig {
//
//    //日志
//    private static Logger log = LoggerFactory.getLogger(ESResourceIndexPropertyXmlConfig.class);
//
//    public static final String CONFIG_FILE = "es_resource_mapping.xml";
//
//    private List<ESIndexProperty> indexPropertyList = new ArrayList<ESIndexProperty>();
//
//    private Set<String> globalResFiledSet = new LinkedHashSet<String>();
//
//    private Map<String,String> indexTypeMap = new HashMap<String,String>();
//
//    private static ESResourceIndexPropertyXmlConfig instance = new ESResourceIndexPropertyXmlConfig();
//
//    public static ESResourceIndexPropertyXmlConfig getInstance(){
//        return instance;
//    }
//
//    private ESResourceIndexPropertyXmlConfig(){
//        loadConfig();
//        startMessageListener();
//    }
//
//
//
//    private void loadConfig() {
//        InputStream in = null;
//        Reader reader = null;
//        try {
//            try {
//                byte[] bytes = RMIUtil.getCommonAPI().loadBytesFromServer("conf/" + CONFIG_FILE);
//                if (bytes != null && bytes.length > 0) {
//                    in = new ByteArrayInputStream(bytes);
//                }
//            } catch (Exception ex) {
//                log.error("get conf/" + CONFIG_FILE + " from redis catch an exception", ex);
//            }
//
//            if(in == null){
//                log.error("can't get conf/" + CONFIG_FILE + " from redis");
//            }
//
//            reader = new InputStreamReader(in);
//
//            SAXReader sax = new SAXReader();
//            Document doc = sax.read(reader);
//            init(doc);
//        } catch (Throwable ex) {
//            log.error("load file catch an exception [" + CONFIG_FILE + "]", ex);
//        } finally {
//            CommonIOUtils.close(reader, in);
//        }
//    }
//
//
//    /**
//     * 初始化配置
//     *
//     * @param doc
//     * @throws Exception
//     */
//    private void init(Document doc) throws Throwable {
//        Element cmcElement = (Element) doc.selectSingleNode("/config");
//
//        Element resourceElement = (Element) cmcElement.selectSingleNode("resource");
//        this.initRsourceElement(resourceElement);
//    }
//
//    /**
//     * 初始化资源索引字段参数
//     * @param resourceElement
//     * @throws Exception
//     */
//    private void initRsourceElement(Element resourceElement) throws Exception{
//        //global
//        Element globalElement = (Element) resourceElement.selectSingleNode("global");
//        List<ESIndexProperty> globalResIndexPropertyList = this.getESIndexPropertyListByElement(globalElement);
//        if(globalResIndexPropertyList != null && globalResIndexPropertyList.size() >0){
//            this.indexPropertyList.addAll(globalResIndexPropertyList);
//            this.globalResFiledSet = this.getResFieldByIndexPropertyList(globalResIndexPropertyList);
//        }
//    }
//
//
//    /**
//     * 获取索引定义
//     * @param perpertyElementList
//     * @return
//     * @throws Exception
//     */
//    private List<ESIndexProperty> getESIndexPropertyListByElement(Element element) throws Exception{
//        List perpertyElementList = element.selectNodes("property");
//        List<ESIndexProperty> list = new ArrayList<ESIndexProperty>();
//        for(int i = 0; i < perpertyElementList.size(); i++){
//            Element propertyElement = (Element)perpertyElementList.get(i);
//            ESIndexProperty indexProperty = this.getESIndexPropertyByElement(propertyElement);
//            list.add(indexProperty);
//
//            indexTypeMap.put(indexProperty.getName(), indexProperty.getType());
//        }
//
//        return list;
//    }
//
//    /**
//     * 获取单个索引定义
//     * @param propertyElement
//     * @return
//     * @throws Exception
//     */
//    private ESIndexProperty getESIndexPropertyByElement(Element propertyElement) throws Exception{
//        String name = propertyElement.attributeValue("name");
//        String type = propertyElement.attributeValue("type");
//        String index = propertyElement.attributeValue("index");
//
//        ESIndexProperty indexProperty = new ESIndexProperty(name,type,index);
//
//        return indexProperty;
//    }
//
//
//    /**
//     * 根据索引定义获取资源属性列表
//     * @param list
//     * @return
//     */
//    private Set<String> getResFieldByIndexPropertyList(List<ESIndexProperty> list){
//        Set<String> set = new LinkedHashSet<String>();
//        for(int i = 0; i < list.size(); i++){
//            ESIndexProperty indexProperty = list.get(i);
//            set.add(indexProperty.getName());
//        }
//
//        return set;
//    }
//
//    public List<ESIndexProperty> getIndexPropertyList() {
//        return indexPropertyList;
//    }
//
//    /**
//     * 获取索引mapping
//     * @return
//     */
//    public Map<String, ESIndexProperty> getESIndexPropertyMap() {
//        Map<String, ESIndexProperty> map = new HashMap<String, ESIndexProperty>();
//        List<ESIndexProperty> list = getIndexPropertyList();
//        if (list != null) {
//            for (ESIndexProperty indexProperty : list) {
//                if (indexProperty == null) {
//                    continue;
//                }
//                map.put(indexProperty.getName(), indexProperty);
//            }
//        }
//        return map;
//    }
//
//    /**
//     * 接收索引重新创建装载的消息
//     */
//    private void startMessageListener() {
//    	 log.info("Start message listener, topic=MessageState.CONFIG_TOPIC");
//        try
//        {
//        	MessageClient.getInstance().addMessageHandler(MessageState.CONFIG_RULE_TOPIC, new MessageHandler() {
//                @Override
//                public void onMessage(String msg) {
//                    Message msgObject = null;
//                    try {
//                        if(msg == null && "".equals(msg)){
//                            return;
//                        }else{
//                            ObjectMapper objectMapper = new ObjectMapper();
//                            msgObject = objectMapper.readValue(msg, Message.class);
//                        }
//                        if (msgObject.getType() == MessageType.ES_INDEX_FIELD_UPDATE) {
//                             Object obj = msgObject.getUserObject();
//                             if(ESMappingManager.ESMAPPING_RESOURCE.equalsIgnoreCase(obj+""))
//                             {
//                                 log.info("Start loadConfig : " + CONFIG_FILE);
//                                 loadConfig();
//                             }
//                        }
//                    } catch (JsonProcessingException e) {
//                        e.printStackTrace();
//                    }
//                }
//            });
//        } catch (Exception e) {
//            log.error("Start message listener error,topic=MessageState.PERFORM_TOPIC", e);
//        }
//
//
//    }
//
//    /**
//     * 获取索引类型
//     *
//     * @return
//     */
//    public Map<String, String> getIndexTypeMap() {
//        return indexTypeMap;
//    }
//}
//
