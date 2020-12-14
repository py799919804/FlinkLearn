package elasticsearch.mapping;//package com.ultrapower.fsms.common.elasticsearch.mapping;
//
//import com.ultrapower.fsms.common.elasticsearch.ESIndexProperty;
//import com.ultrapower.fsms.common.dubbo.RMIUtil;
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
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class IndexPropertyXmlConfig {
//
//	private static Logger log = LoggerFactory.getLogger(IndexPropertyXmlConfig.class);
//
//	public List<ESIndexProperty> indexPropertyList = new ArrayList<ESIndexProperty>();
//
//	private Map<String, String> indexTypeMap = new HashMap<String, String>();
//
//	/**
//	 * 初始化配置
//	 *
//	 * @param doc
//	 * @throws Exception
//	 */
//	public void init(Document doc) throws Throwable {
//		Element cmcElement = (Element) doc.selectSingleNode("/config");
//		this.initRsourceElement(cmcElement);
//	}
//
//	/**
//	 * 初始化告警索引字段参数
//	 *
//	 * @param faultElement
//	 * @throws Exception
//	 */
//	public void initRsourceElement(Element faultElement) throws Exception {
//		// global
//		Element globalElement = (Element) faultElement.selectSingleNode("global");
//		List<ESIndexProperty> globalResIndexPropertyList = this.getESIndexPropertyListByElement(globalElement);
//		if (globalResIndexPropertyList != null && globalResIndexPropertyList.size() > 0) {
//			this.indexPropertyList.addAll(globalResIndexPropertyList);
//		}
//	}
//
//	/**
//	 * 获取索引定义
//	 *
//	 * @param perpertyElementList
//	 * @return
//	 * @throws Exception
//	 */
//	public List<ESIndexProperty> getESIndexPropertyListByElement(Element element) throws Exception {
//		List perpertyElementList = element.selectNodes("property");
//		List<ESIndexProperty> list = new ArrayList<ESIndexProperty>();
//		for (int i = 0; i < perpertyElementList.size(); i++) {
//			Element propertyElement = (Element) perpertyElementList.get(i);
//			ESIndexProperty indexProperty = this.getESIndexPropertyByElement(propertyElement);
//			list.add(indexProperty);
//
//			indexTypeMap.put(indexProperty.getName(), indexProperty.getType());
//		}
//
//		return list;
//	}
//
//	/**
//	 * 获取单个索引定义
//	 *
//	 * @param propertyElement
//	 * @return
//	 * @throws Exception
//	 */
//	public ESIndexProperty getESIndexPropertyByElement(Element propertyElement) throws Exception {
//		String name = propertyElement.attributeValue("name");
//		String type = propertyElement.attributeValue("type");
//		String index = propertyElement.attributeValue("index");
//
//		ESIndexProperty indexProperty = new ESIndexProperty(name, type, index);
//
//		return indexProperty;
//	}
//
//	public List<ESIndexProperty> getIndexPropertyList() {
//		return indexPropertyList;
//	}
//
//	/**
//	 * 获取索引mapping
//	 *
//	 * @return
//	 */
//	public Map<String, ESIndexProperty> getESIndexPropertyMap() {
//		Map<String, ESIndexProperty> map = new HashMap<String, ESIndexProperty>();
//		List<ESIndexProperty> list = getIndexPropertyList();
//		if (list != null) {
//			for (ESIndexProperty indexProperty : list) {
//				if (indexProperty == null) {
//					continue;
//				}
//				map.put(indexProperty.getName(), indexProperty);
//			}
//		}
//		return map;
//	}
//
//	/**
//	 * 获取索引类型
//	 *
//	 * @return
//	 */
//	public Map<String, String> getIndexTypeMap() {
//		return indexTypeMap;
//	}
//
//	public void loadConfig(String fileName) {
//		InputStream in = null;
//		Reader reader = null;
//		try {
//			try {
//				byte[] bytes = RMIUtil.getCommonAPI().loadBytesFromServer("conf/" + fileName);
//				if (bytes != null && bytes.length > 0) {
//					in = new ByteArrayInputStream(bytes);
//				}
//			} catch (Exception ex) {
//				log.error("get conf/" + fileName + " from redis catch an exception", ex);
//			}
//
//			if (in == null) {
//				log.error("can't get conf/" + fileName + " from redis");
//			}
//
//			reader = new InputStreamReader(in);
//
//			SAXReader sax = new SAXReader();
//			Document doc = sax.read(reader);
//			init(doc);
//		} catch (Throwable ex) {
//			log.error("load file catch an exception [" + fileName + "]", ex);
//		} finally {
//			CommonIOUtils.close(reader, in);
//		}
//	}
//}
