package flink.streaming;

import flink.streaming.bean.BizData;
import flink.streaming.common.ConverUtils;
import flink.streaming.common.IdGenerator;
import flink.streaming.elasticsearch.FlinkBulkElasticsearch;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


/**
 * @author caijinpeng
 * @Titile
 * @Description
 * @date 2020/2/7 15:42
 */
public class KafkaMsgStreamingSinkES {
    private static Logger logger = LoggerFactory.getLogger(KafkaMsgStreamingSinkES.class);

    /** kafka消费者的安全认证配置 （admin/ultra#12p39）**/
    public static final String consumer_jaas_config = "org.apache.kafka.common.security.plain.PlainLoginModule required \r\n" +
            "username=\"admin\"\r\n" +
            "password=\"ultra#12p39\";";

    //要加载的配置文件
    private static final String CONFIG_NAME = "/config.properties";

    private static final IdGenerator idGen = IdGenerator.getInstance();


    public static void main(String[] args) throws Exception {

        // 读取本地文件
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(KafkaMsgStreamingSinkES.class.getResourceAsStream(CONFIG_NAME));
        String kafkaurl = parameterTool.getRequired("kafka.url");
        String kafkatopic = ConverUtils.Obj2Str(parameterTool.getRequired("kafka.topic"), "qBizDataPMJson");
        int kafkaMaxpollRecords = ConverUtils.Obj2int(parameterTool.getRequired("kafka.max.poll.records"), 30000);

        int flinkParallelism = ConverUtils.Obj2int(parameterTool.getRequired("flink.parallelism"), 8);

        // StreamAPI环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 非常关键，一定要设置启动检查点！！
        ///env.enableCheckpointing(5000);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //env.getConfig().setAutoWatermarkInterval(100);

        env.getConfig().setTaskCancellationTimeout(120*1000);

        env.setParallelism(flinkParallelism);


        // kafka设置参数
        Properties props = new Properties();
        ///props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  "192.168.95.117:58954,192.168.181.83:58954,192.168.181.84:58954");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  kafkaurl);
        // enable.auto.commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // auto.commit.interval.ms
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        // 1秒发送一次心跳，此值要小于session.timeout.ms的值
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");
        // 5秒kafka如果没有收到心跳，认为Consumer不在线，将发生再平衡
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        // 服务端 至少有多少数据 才返回（按 byte去计算）
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "5");
        // 没有给出的 fetch.min.bytes 的足够数据, 阻塞最大时长
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        // 每次读取的数据量限制 30000
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(kafkaMaxpollRecords));

        props.setProperty("group.id", "kafka-flink-caijinpeng-test1");



        // 传入的是kafka中的topic
        FlinkKafkaConsumer011 consumer011 =new FlinkKafkaConsumer011<>(kafkatopic, new SimpleStringSchema(), props);
        ///consumer011.setStartFromGroupOffsets();
        consumer011.setStartFromEarliest();

        DataStream<String> stream = env.addSource(consumer011);


        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.setSerializationInclusion(JsonInclude.Include.ALWAYS);
//        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        /** 简单转换加载*/
        SingleOutputStreamOperator<BizData> etl = stream.process(new ProcessFunction<String, BizData>() {
            @Override
            public void processElement(String value, Context ctx, Collector<BizData> out) {
                long nowTime = System.currentTimeMillis();
                BizData data = null;
                try {
                    if(null!=value && value.trim().length()>0){
                        data = objectMapper.readValue(value, BizData.class);
                    }
                    data.setReceiveTime(nowTime);
                } catch (Exception ex) {
                    logger.error("json2BizData error!", ex);
                }

                if (null == data || data.getDcTime()==0) {
                    logger.warn("BizData is null ! ");
                    return;
                }

                out.collect(data);
            }
        });


        String esUrl = parameterTool.getRequired("es.url");
        String esCluster = ConverUtils.Obj2Str(parameterTool.getRequired("es.clustername"), "es760");
        int maxActions = ConverUtils.Obj2int(parameterTool.getRequired("es.maxactions"), 5000);

//        // 构造ElasticsearchSinkBuilder
//        ElasticsearchSink elasticsearchSink = FlinkToElasticsearch7.getElasticsearchSink(esUrl, esCluster, maxActions);
//        etl.addSink(elasticsearchSink).name("esSink").setParallelism(flinkParallelism);


        etl.addSink(new FlinkBulkElasticsearch(esUrl, esCluster, maxActions)).name("esSink1").setParallelism(flinkParallelism);



        env.execute("Kafka-Flink-ES-pengyue-test");
    }


}
