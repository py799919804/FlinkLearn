package redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPoolConfig;

/**
 * 〈redis集群默认配置〉
 *
 * @author lijianye
 * @create 2020/3/10
 * @since 1.0.0
 */
public class DefaultJedisConfig {

    private static Logger log = LoggerFactory.getLogger(DefaultJedisConfig.class);

    public static final int DEFAULT_MAXACTIVE = 5000;
    public static final int DEFAULT_MAXIDLE = 5000;
    public static final int DEFAULT_MAXWAIT = 10000;
    public static final int DEFAULT_TIMEOUT = 60000;

    public static int maxActive = DEFAULT_MAXACTIVE; // 最大活动连接数
    public static int maxIdle = DEFAULT_MAXIDLE; // 最大空闲连接数
    public static int maxWait = DEFAULT_MAXWAIT; // 当无可用连接时，最大等待时间
    public static boolean testOnBorrow = false;
    public static int clientTimeOutMills = DEFAULT_TIMEOUT; // 单位毫秒

    public static boolean isInited = false;

    /**
     * 获取默认的Jedis连接池配置
     *
     * @return
     */
    public synchronized static JedisPoolConfig getDefaultJedisPoolConfig() {
        if (isInited == false) {
            isInited = true;
            init();
        }

        JedisPoolConfig config = new JedisPoolConfig();

        // 连接耗尽时是否阻塞, false报异常,ture阻塞直到超时, 默认true
        config.setBlockWhenExhausted(true);

        // 设置的逐出策略类名, 默认DefaultEvictionPolicy(当连接超过最大空闲时间,或连接数超过最大空闲连接数)
        config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy");

        // 是否启用pool的jmx管理功能, 默认true
        config.setJmxEnabled(true);

        // MBean ObjectName = new ObjectName("org.apache.commons.pool2:type=GenericObjectPool,name=" + "pool" + i); 默 认为"pool",
        // JMX不熟,具体不知道是干啥的...默认就好.
        config.setJmxNamePrefix("pool");

        // 是否启用后进先出, 默认true
        config.setLifo(true);

        // 最大空闲连接数, 默认8个
        config.setMaxIdle(maxIdle);

        // 最大连接数, 默认8个
        config.setMaxTotal(maxActive);

        // 获取连接时的最大等待毫秒数(如果设置为阻塞时BlockWhenExhausted),如果超时就抛异常, 小于零:阻塞不确定的时间, 默认-1
        config.setMaxWaitMillis(maxWait);

        // 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
        config.setMinEvictableIdleTimeMillis(60000);

        // 最小空闲连接数, 默认0
        config.setMinIdle(10);

        // 每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
        config.setNumTestsPerEvictionRun(150);

        // 对象空闲多久后逐出, 当空闲时间>该值 且 空闲连接>最大空闲数 时直接逐出,不再根据MinEvictableIdleTimeMillis判断 (默认逐出策略)
        config.setSoftMinEvictableIdleTimeMillis(60000);

        // 在获取连接的时候检查有效性, 默认false
        config.setTestOnBorrow(false);

        config.setTestOnReturn(true);

        // 在空闲时检查有效性, 默认false
        config.setTestWhileIdle(true);

        // 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
        config.setTimeBetweenEvictionRunsMillis(30000);

        return config;
    }

    /**
     * 从系统配置中读取
     */
    private static void init() {
        try {
            try {
                maxActive = Integer.parseInt(System.getProperty(RedisConstant.REDIS_MAXACTIVE));
            } catch (Exception ex) {
                maxActive = DEFAULT_MAXACTIVE;
            }
            if (maxActive < 0) {
                maxActive = DEFAULT_MAXACTIVE;
            }

            try {
                maxIdle = Integer.parseInt(System.getProperty(RedisConstant.REDIS_MAXIDLE));
            } catch (Exception ex) {
                maxIdle = DEFAULT_MAXIDLE;
            }
            if (maxIdle < 0) {
                maxIdle = DEFAULT_MAXIDLE;
            }

            try {
                maxWait = Integer.parseInt(System.getProperty(RedisConstant.REDIS_MAXWAIT));
            } catch (Exception ex) {
                maxWait = DEFAULT_MAXWAIT;
            }
            if (maxWait < 0) {
                maxWait = DEFAULT_MAXWAIT;
            }

            try {
                clientTimeOutMills = Integer.parseInt(System.getProperty(RedisConstant.REDIS_TIMEOUT));
            } catch (Exception ex) {
                clientTimeOutMills = DEFAULT_TIMEOUT;
            }
            if (clientTimeOutMills < 0) {
                clientTimeOutMills = DEFAULT_TIMEOUT;
            }

            log.info("maxActive=" + maxActive + ",maxIdle=" + maxIdle + ",maxWait=" + maxWait + ",clientTimeOutMills=" + clientTimeOutMills);
        } catch (Exception ex) {
            log.error("redisparam init catch an exception", ex);
        }
    }

}

    