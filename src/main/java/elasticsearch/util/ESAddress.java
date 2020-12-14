package elasticsearch.util;
/**
 * @Title:
 *
 * @description:
 * 
 * @Company: ultrapower.com
 * @author lnj2050 
 * @create time：2016年5月11日  上午11:09:25
 * @version 1.0
 */
public class ESAddress {
    
    private String ip;
    
    private int port;
    
    
    public ESAddress() {
    }

    public ESAddress(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ElasticsearchAddress [ip=");
        builder.append(ip);
        builder.append(", port=");
        builder.append(port);
        builder.append("]");
        return builder.toString();
    }
}

