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
//import com.ultrapower.fsms.common.utils.StringUtils;
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
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//
///**
// * @Title:
// *   性能表ES配置属性读取
// * @description:
// *
// * @Company: ultrapower.com
// * @author lnj2050
// * @create time：2016年5月12日  下午3:43:14
// * @version 1.0
// */
//public class ESPmIndexPropertyXmlConfig {
//
//    //日志
//    private static Logger log = LoggerFactory.getLogger(ESPmIndexPropertyXmlConfig.class);
//
//    public static final String CONFIG_FILE = "es_pm_mapping.xml";
//
//    private List<ESIndexProperty> globalPmIndexFields = new ArrayList<ESIndexProperty>();
//
//    private List<ESIndexProperty> globalResFields = new ArrayList<ESIndexProperty>();
//
//    private Map<String,List<ESIndexProperty>> exResFields = new ConcurrentHashMap<String,List<ESIndexProperty>>();
//
//    private Map<String,List<ESIndexProperty>> allPmIndexFields = new ConcurrentHashMap<String,List<ESIndexProperty>>();
//
//    private static ESPmIndexPropertyXmlConfig instance = new ESPmIndexPropertyXmlConfig();
//
//    public static ESPmIndexPropertyXmlConfig getInstance(){
//        return instance;
//    }
//
//    private ESPmIndexPropertyXmlConfig(){
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
//        Element pmElement = (Element) cmcElement.selectSingleNode("pm");
//        this.initPmElement(pmElement);
//
//        //Element resourceElement = (Element) cmcElement.selectSingleNode("resource");
//        //this.initRsourceElement(resourceElement);
//    }
//
//
//    /**
//     * 初始化pm索引字段参数
//     *
//     * @param pmElement
//     */
//    private void initPmElement(Element pmElement) throws Exception {
//        this.globalPmIndexFields = this.getESIndexPropertyListByElement(pmElement);
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
//        globalResFields = this.getESIndexPropertyListByElement(globalElement);
//        if(globalResFields != null && globalResFields.size() >0){
//            this.globalPmIndexFields.addAll(globalResFields);
//        }
//
//        //class
//        List classtitleElementList = resourceElement.selectNodes("classtitle");
//        for(int i = 0; i < classtitleElementList.size(); i++){
//            Element classElement = (Element)classtitleElementList.get(i);
//            String classname = classElement.attributeValue("name");
//            if(StringUtils.isEmpty(classname)){
//                continue;
//            }
//
//            List<ESIndexProperty> list = this.getESIndexPropertyListByElement(classElement);
//            if(list == null || list.size() <= 0){
//                continue;
//            }
//
//            String key = classname.toLowerCase();
//            List<ESIndexProperty> allList = new ArrayList<ESIndexProperty>();
//            allList.addAll(this.globalPmIndexFields);
//            allList.addAll(list);
//            this.allPmIndexFields.put(key,allList);
//            list.addAll(this.globalResFields);
//            this.exResFields.put(key, list);
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
//        ESIndexProperty indexProperty = new ESIndexProperty(name,type,index);
//
//        String resProperty = propertyElement.attributeValue("res_property");
//        if (resProperty!=null) {
//        	indexProperty.setResProperty(resProperty);
//        }
//        String resPropertyFrom = propertyElement.attributeValue("res_property_from");
//        if (resPropertyFrom!=null) {
//        	indexProperty.setResPropertyFrom(resPropertyFrom);
//        }
//
//        // 界面显示时使用的资源属性名称
//        String resDisplayProperty = propertyElement.attributeValue("res_display_property");
//        indexProperty.setResDisplayProperty(resDisplayProperty);
//
//
//
//        String isdim = propertyElement.attributeValue("isdim");
//
//        if(isdim!=null &&! isdim.trim().equals(""))
//        {
//          indexProperty.setIsdim(Boolean.parseBoolean(isdim));
//        }
//
//        // 维度资源类型名
//        String resClassTitle = propertyElement.attributeValue("res_classtitle");
//        indexProperty.setResClassTitle(resClassTitle);
//
//
//        return indexProperty;
//    }
//
//    /**
//     * 根据classtitle获取性能数据索引定义
//     * @param classtitle
//     * @return
//     */
//    public List<ESIndexProperty> getPmIndexPropertyListByClassTitle(String classtitle){
//        String key = classtitle.toLowerCase();
//        if(this.allPmIndexFields.containsKey(key)){ //配置了扩展资源类型
//            return this.allPmIndexFields.get(key);
//        }else{
//            return this.globalPmIndexFields;
//        }
//    }
//
//
//    /**
//     * 根据classtitle获取进行索引的资源属性字段
//     * @param classtitle
//     * @return
//     */
////    public List<ESIndexProperty> getResFieldsByClassTitle(String classtitle){
////        String key = classtitle.toLowerCase();
////        if(this.exResFields.containsKey(key)){ //配置了扩展资源类型
////            return this.exResFields.get(key);
////        }else{
////            return this.globalResFields;
////        }
////    }
//
//    /**
//     * 接收索引重新创建装载的消息
//     */
//    private void startMessageListener() {
//    	 log.info("Start message listener, topic=MessageState.PERFORM_TOPIC");
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
//
//                        if (msgObject.getType() == MessageType.ES_INDEX_FIELD_UPDATE) {
//                             Object obj = msgObject.getUserObject();
//                             if(ESMappingManager.ESMAPPING_PM.equalsIgnoreCase(obj+""))
//                             {
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
//
//
//}
//
