//package redis;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import redis.clients.jedis.HostAndPort;
//import redis.clients.jedis.JedisCluster;
//import redis.clients.jedis.JedisPoolConfig;
//
//import java.util.HashSet;
//import java.util.Set;
//
///**
// * 〈redis cluster 连接〉
// *
// * @author lijianye
// * @create 2020/3/10
// * @since 1.0.0
// */
//public class JedisClusterUtil {
//
//    private static Logger log = LoggerFactory.getLogger(JedisClusterUtil.class);
//
//    JedisClusterUtil(){
//
//    }
//
//    public static JedisCluster getJedisCluster() {
//        return RedisClusterPoolHolder.getInstance();
//    }
//
//
//
//    private static final class RedisClusterPoolHolder {
//
//        //使用pool单例
//        private static final ClusterPool CLUSTER_POOL = new ClusterPool();
//
//        private RedisClusterPoolHolder() {
//        }
//
//        private static JedisCluster getInstance() {
//            return CLUSTER_POOL.getJedisCluster();
//        }
//
//    }
//
//    private static class ClusterPool {
//
//        // JedisCluster
//        private static JedisCluster JEDIS_CLUSTER = null;
//
//        ClusterPool() {
//            String clusterUrl = null;
//            String clusterPwd = null;
//            try {
//                // 集群地址和密码从zk获取
////                clusterUrl = ConverUtils.Obj2Str(System.getProperty("redis.url"),"");
//                clusterUrl = "192.168.182.235:6379,192.168.182.236:6379,192.168.182.237:6379";
////                clusterPwd = ConverUtils.Obj2Str(System.getProperty("redis.pwd"),"");
//                clusterPwd = "";
//                if(clusterUrl==null || clusterUrl.trim().equals("")){
//                    log.warn("clusterUrl is null!!!!!!");
//                    return;
//                }
//
//                Set<HostAndPort> hostAndPortsSet = new HashSet<HostAndPort>();
//                String[] redisNodes = clusterUrl.split(",");
//                for(String redisNode : redisNodes){
//                    if(redisNode==null || redisNode.equals("")){
//                        continue;
//                    }
//                    String[] ipPort = redisNode.split(":");
//                    String ip = ipPort[0];
//                    int port = Integer.parseInt(ipPort[1]);
//                    hostAndPortsSet.add(new HostAndPort(ip, port));
//                }
//
//                // 配置JedisPool
//                JedisPoolConfig jedisPoolConfig = DefaultJedisConfig.getDefaultJedisPoolConfig();
//
//                if(clusterPwd==null || clusterPwd.trim().equals("")){
//                    JEDIS_CLUSTER = new JedisCluster(hostAndPortsSet, jedisPoolConfig);
//                } else {
//                    // connectionTimeout 连接超时（毫秒）
//                    // soTimeout 响应超时（毫秒）
//                    // maxAttempts 出现异常最大重试次数
//                    JEDIS_CLUSTER = new JedisCluster(hostAndPortsSet, 5000, 3000, 3, clusterPwd, jedisPoolConfig);
//                }
//            } catch (Exception e) {
//                log.error("new JedisCluster error, clusterUrl="+clusterUrl );
//            }
//        }
//
//        private JedisCluster getJedisCluster() {
//            return JEDIS_CLUSTER;
//        }
//    }
//}
//
//