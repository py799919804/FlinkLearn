package redis;

import redis.clients.jedis.JedisCluster;

public class PyJedisTool {
    public static void main(String[] args) {
        JedisCluster jedisCluster = JedisClusterUtil.getJedisCluster();

    }
}
