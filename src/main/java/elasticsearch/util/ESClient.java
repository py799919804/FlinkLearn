package elasticsearch.util;

import com.ultrapower.fsms.common.utils.StringUtils;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * @Title:
 *  Elasticsearch客户端访问工具类
 * @description:
 * 
 * @Company: ultrapower.com
 * @author lnj2050 
 * @create time：2016年5月11日  上午10:35:38
 * @version 1.0
 */
public class ESClient {
    
    //日志
    private static Logger log = LoggerFactory.getLogger(ESClient.class);
    
    private TransportClient client = null;
    
    //默认集群名称
    public static final String DEFAULT_CLUSTER_NAME = "sigmac";
    
    private String es_url = null;
    
    private String clustername = DEFAULT_CLUSTER_NAME;
    
    /**
     * @param es_url es地址,多个地址之间以,分隔
     */
    public ESClient(String es_url){
        this.es_url = es_url;
        this.init();
    }
    
    /**
     * @param es_url es地址,多个地址之间以,分隔
     * @param clustername 集群名称
     */
    public ESClient(String es_url,String clustername){
        this.es_url = es_url;
        this.clustername = clustername;
        this.init();
    }
    
    
    private void init(){
        if(client == null){
            client = this.buildTransportClient();
        }
    }
    
    
    /**
     * 构建连接客户端
     * @return
     */
    private TransportClient buildTransportClient(){
        if(StringUtils.isEmpty(es_url)){
            log.error("es_url is null");
            return null;
        }
        
        
        List<ESAddress> list = this.parseElasticsearchAddressList(es_url);
        if(list == null || list.size() <= 0){
            log.error("parse es_url=["+es_url+"] error");
            return null;
        }
        
        
        TransportClient client = null;
        try{
            // 是否有权限验证
            String espassword = RMIUtil.getServiceContext().getProperty(IServiceContext.key_es_password);
            Builder setbuilder = Settings.builder().put("client.transport.sniff", true).put("client.transport.ping_timeout", "120s")
                    .put("cluster.name", this.clustername);
            if (espassword == null || espassword.trim().equals("")) {
                Settings settings = setbuilder.build();
                client = new PreBuiltTransportClient(settings);
            } else {
                Settings settings = setbuilder.put("xpack.security.user", "elastic:" + espassword).build();
                client = new PreBuiltXPackTransportClient(settings);

            }

            for(int i = 0; i < list.size(); i++){
                ESAddress address = list.get(i);
                client = client.addTransportAddress(new TransportAddress(new InetSocketAddress(address.getIp(),address.getPort())));
            }
        }catch(Throwable tr){
            log.error("buildTransportClient by ["+this.es_url+"] catch an exception",tr);
        }

        return client;
    }
    
    
    /**
     * 解析es配置地址
     * @param es_url
     * @return
     */
    private List<ESAddress> parseElasticsearchAddressList(String es_url){
        String []arr = es_url.split(",");
        List<ESAddress> list = new ArrayList<ESAddress>();
        for(int i = 0; i < arr.length; i++){
            String sigleAddress = arr[i];
            String [] addArr = sigleAddress.split(":");
            String ip = addArr[0];
            int port = Integer.parseInt(addArr[1]);
            ESAddress address = new ESAddress(ip,port);
            list.add(address);
        }
        
        return list;
    }

    public TransportClient getClient() {
        return client;
    }

}

