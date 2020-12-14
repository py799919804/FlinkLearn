package elasticsearch;//package com.ultrapower.fsms.common.elasticsearch;
//
//import com.ultrapower.fsms.common.dubbo.RMIUtil;
//import com.ultrapower.fsms.common.servicecontext.IServiceContext;
//import com.ultrapower.fsms.common.sysprop.SystemPropertityUtil;
//import com.ultrapower.fsms.common.utils.CacheManager;
//import org.apache.commons.codec.binary.Base64;
//import org.apache.http.HttpEntity;
//import org.apache.http.HttpResponse;
//import org.apache.http.HttpStatus;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.CloseableHttpResponse;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.client.methods.HttpPost;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClients;
//import org.elasticsearch.common.transport.TransportAddress;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.*;
//import java.math.BigDecimal;
//import java.net.URLEncoder;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * @Title:
// *
// * @description:
// *
// * @Company: ultrapower.com
// * @author ljy
// * @create time：2016年5月30日 下午5:24:45
// * @version 1.0
// */
//public class ESSqlHandler {
//
//    private static Logger log = LoggerFactory.getLogger(ESSqlHandler.class);
//
//    /**
//     * 获取多个字段的结果集
//     * @param essql (limit0,10000 最大10000)
//     * @return
//     */
//    public List<Map> getQueryMoreFieldResult(String essql) {
//        List<Map> results = new ArrayList<Map>();
//        String separator = ",";
//        String httpUrl = getHttpUrl(essql, separator);
//        CloseableHttpClient httpclient = HttpClients.createDefault();
//        HttpPost httppost = getHttpPost(httpUrl);
//        CloseableHttpResponse response = null;
//        try {
//            response = httpclient.execute(httppost);
//            int error = response.getStatusLine().getStatusCode();
//            if (error == 500) {
//                log.error("getQueryMoreFieldResult ES Sql语句错误请检查：essql = " + essql + "; httpUrl = " + httpUrl);
//            }
//            // 查询es所得数据
//            HttpEntity entity = response.getEntity();
//            // 流获取返回的数据主体内容.
//            InputStream instream = entity.getContent();
//            BufferedReader br = new BufferedReader(new InputStreamReader(instream, "utf-8"));
//            String str = "";
//            int i = 0;
//            String[] fieldary = null;
//            // 循环每一行数据
//            while ((str = br.readLine()) != null) {
//                if (i == 0) {
//                    fieldary = str.split(separator);
//                    i++;
//                    continue;
//                }
//                if (fieldary != null) {
//                    String[] valueary = str.split(separator,-1);
//                    if (fieldary.length == valueary.length) {
//                        Map fieldMap = new HashMap();
//                        for (int j = 0; j < fieldary.length; j++) {
//                            String tempvalue = valueary[j];
//                            if (tempvalue != null && tempvalue.equals("-Infinity")) {
//                                tempvalue = null;
//                            }
//                            fieldMap.put(fieldary[j], tempvalue);
//                        }
//                        results.add(fieldMap);
//                    }
//                }
//            }
//        } catch (Exception e) {
//            log.error(" execute sql error;essql = " + essql, e);
//        } finally {
//            // 关闭连接,释放资源
//            try {
//                response.close();
//                httpclient.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        return results;
//    }
//
//    /**
//     * 根据es sql 获取资源id
//     *
//     * @param essql
//     * @return
//     */
//    public List<String> getQueryResult(String essql) {
//        List<String> resids = new ArrayList<String>();
//        String httpUrl = getHttpUrl(essql, null);
//        CloseableHttpClient httpclient = HttpClients.createDefault();
//        HttpPost httppost = getHttpPost(httpUrl);
//        CloseableHttpResponse response = null;
//        try {
//            response = httpclient.execute(httppost);
//            int error = response.getStatusLine().getStatusCode();
//            if (error == 500) {
//                log.error("getQueryResult ES Sql语句错误请检查：essql = " + essql + "; httpUrl = " + httpUrl);
//            }
//            // 查询es所得数据
//            HttpEntity entity = response.getEntity();
//            // 流获取返回的数据主体内容.
//            InputStream instream = entity.getContent();
//            BufferedReader br = new BufferedReader(new InputStreamReader(instream, "utf-8"));
//            String str = "";
//            int i = 0;
//            // 循环每一行数据
//            while ((str = br.readLine()) != null) {
//                if (i == 0) {
//                    i++;
//                    continue;
//                }
//                resids.add(str);
//            }
//        } catch (Exception e) {
//            log.error(" execute sql error;essql = " + essql, e);
//        } finally {
//            // 关闭连接,释放资源
//            try {
//                response.close();
//                httpclient.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        return resids;
//    }
//
//    /**
//     * 获取统计信息
//     *
//     * @param essql
//     * @return
//     */
//    public Map<String, String> getFacetResult(String essql) {
//        Map<String, String> map = new HashMap<String, String>();
//        String httpUrl = getHttpUrl(essql,null);
//        CloseableHttpClient httpclient = HttpClients.createDefault();
//        HttpPost httppost = getHttpPost(httpUrl);
//        CloseableHttpResponse response = null;
//        try {
//            response = httpclient.execute(httppost);
//            int error = response.getStatusLine().getStatusCode();
//            if (error == 500) {
//                log.error("getFacetResult Sql语句错误请检查：essql = " + essql + "; httpUrl = " + httpUrl);
//            }
//            // 查询es所得数据
//            HttpEntity entity = response.getEntity();
//            // 流获取返回的数据主体内容.
//            InputStream instream = entity.getContent();
//            BufferedReader br = new BufferedReader(new InputStreamReader(instream, "utf-8"));
//            String str = "";
//            int i = 0;
//            // 循环每一行数据
//            while ((str = br.readLine()) != null) {
//                if (i == 0) {
//                    i++;
//                    continue;
//                }
//                String[] tempary = str.split(",");
//                String key = tempary[0];
//                String numstr = null;
//                if (tempary.length > 1) {
//                    numstr = tempary[1];
//                }
//
//                if (numstr != null) {
//                    // 科学计数法转数字
//                    if (numstr.indexOf("E") > 0) {
//                        BigDecimal bd = new BigDecimal(numstr);
//                        numstr = bd.toPlainString();
//                    }
//                    // 去掉小数
//                    if (numstr.indexOf(".") > 0) {
//                        numstr = numstr.substring(0, numstr.indexOf("."));
//                    }
//                }
//
//                map.put(key, numstr);
//            }
//        } catch (Exception e) {
//            log.error(" execute sql error;essql = " + essql, e);
//        } finally {
//            // 关闭连接,释放资源
//            try {
//            	if(response!=null) {
//                response.close();
//              }
//            	if(httpclient!=null) {
//                httpclient.close();
//              }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//        return map;
//    }
//
//    /**
//     * 获取es ip地址
//     *
//     * @return
//     */
//    private List<String> getESHost() {
//        List<String> hosts = new ArrayList<String>();
//        List<TransportAddress> iplist = ESClientUtil.getInstance().getClient().transportAddresses();
//        if (iplist != null && iplist.size() > 0) {
//            for (TransportAddress transportAddress : iplist) {
//                hosts.add(transportAddress.getAddress());
//            }
//        }
//        return hosts;
//    }
//
//    /**
//     * 获取http url
//     * @param essql
//     * @return
//     */
//    public String getHttpUrl(String essql, String Separator) {
//        String httpUrl = "";
//        try {
//            essql = URLEncoder.encode(essql, "UTF-8");
//        } catch (UnsupportedEncodingException e1) {
//            log.error("encode error,str=" + essql, e1);
//        }
//
//        httpUrl = getpreHttpUrl();
////        if (Separator != null && !Separator.equals("")) {
////            httpUrl += "separator=" + Separator + "&";
////        }
//        httpUrl += "format=csv&sql=" + essql;
//        return httpUrl;
//    }
//
//    public String getpreHttpUrl() {
//        String httpUrl = "";
//        String cacheKey = "httpurl";
//        Object obj = CacheManager.getData(cacheKey);
//        if (obj != null) {
//            httpUrl = obj.toString();
//        } else {
//            log.info("get es http url...");
//            boolean result = false;
//            String httpPort = SystemPropertityUtil.getStrByPropKey("es_sql_http_port","59200");
//            if (httpPort == null || httpPort.trim().equals("")) {
//                httpPort = "59200";
//            }
//            List<String> hosts = getESHost();
//            for (String host : hosts) {
//                String curl = "http://" + host + ":" + httpPort;
//                result = httpESChecker(curl);//httpChecker(curl);
//                if (result) {
//                    httpUrl = "http://" + host + ":" + httpPort + "/_sql?";
//                    break;
//                }
//                log.warn("please check es server,can not ask " + curl);
//            }
//
//            if (httpUrl != null && !httpUrl.equals("")) {
//                CacheManager.setData(cacheKey, httpUrl, 300); // 5分钟后过期
//            }
//        }
//
//        return httpUrl;
//    }
//
//    /**
//     * 验证http连接
//     *
//     * @param httpUrl
//     * @return
//     */
//    public boolean httpChecker(String httpUrl) {
//    	CloseableHttpClient httpCilent2 = HttpClients.createDefault();
//
//    	boolean connFlag = false;
//        try {
//        	RequestConfig requestConfig = RequestConfig.custom()
//                    .setConnectTimeout(10 * 1000)   //设置连接超时时间
//                    .setConnectionRequestTimeout(10 * 1000) // 设置请求超时时间
//                    .setSocketTimeout(10 * 1000)
//                    .setRedirectsEnabled(true)//默认允许自动重定向
//                    .build();
//            HttpGet httpGet2 = new HttpGet(httpUrl);
//            httpGet2.setConfig(requestConfig);
//
//            HttpResponse httpResponse = httpCilent2.execute(httpGet2);
//            if(httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK){
//            	connFlag = true;
//            }
//        } catch (Exception ex) {
//        	log.error("execute url=" + httpUrl + ", catch an exception" + ex.getMessage());
//        }finally {
//            try {
//                httpCilent2.close();
//            } catch (IOException e) {
//            	 log.error("", e);
//            }
//        }
//        return connFlag;
//    }
//
//    /**
//     * 验证es url是否可用
//     * @param httpUrl
//     * @return
//     */
//	private boolean httpESChecker(String httpUrl) {
//		boolean result = false;
//		String authHeader = getAuthHeader();
//		if (authHeader == null) {
//			result = httpChecker(httpUrl);
//		} else {
//			HttpGet httpget = new HttpGet(httpUrl);
//			httpget.addHeader("Authorization", authHeader);
//			CloseableHttpClient httpclient = HttpClients.createDefault();
//			try {
//				CloseableHttpResponse response = httpclient.execute(httpget);
//				int status = response.getStatusLine().getStatusCode();
//				if (status == HttpStatus.SC_OK) {
//					result = true;
//				}
//			} catch (IOException e2) {
//				log.error("httpESChecker error,url=" + httpUrl, e2);
//			}
//		}
//		return result;
//	}
//
//    /**
//     * 构造Basic Auth认证头信息
//     *
//     * @return
//     */
//	private HttpPost getHttpPost(String httpUrl) {
//		HttpPost httpPost = new HttpPost(httpUrl);
//		String authHeader = getAuthHeader();
//		if (authHeader != null) {
//			httpPost.addHeader("Authorization", authHeader);
//		}
//		return httpPost;
//	}
//
//    public Map getPropertiesMapping(String indexName, String typeName){
//    	ESMappingManager manager = new ESMappingManager();
//        return manager.getPropertiesMapping(indexName, typeName);
//    }
//
//	private String getAuthHeader() {
//		String authHeader = null;
//		String espassword = RMIUtil.getServiceContext().getProperty(IServiceContext.key_es_password);
//		if (espassword != null && !espassword.trim().equals("")) {
//			String auth = "elastic:" + espassword;
//			byte[] encodedAuth = Base64.encodeBase64(auth.getBytes());
//			authHeader = "Basic " + new String(encodedAuth);
//		}
//		return authHeader;
//	}
//}
//
