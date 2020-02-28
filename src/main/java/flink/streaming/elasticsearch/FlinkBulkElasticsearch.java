package flink.streaming.elasticsearch;

import flink.streaming.bean.BizData;
import flink.streaming.common.ConverUtils;
import flink.streaming.common.IdGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author caijinpeng
 * @Titile
 * @Description
 * @date 2020/2/12 13:44
 */
public class FlinkBulkElasticsearch extends RichSinkFunction<BizData> {

    private static final Logger loggr = LoggerFactory.getLogger(FlinkBulkElasticsearch.INDEX);

    private static final String INDEX = "pytest"; // index name
    private static final String INDEX_TYPE = "sigmac"; // document type

    private static final IdGenerator idGen = IdGenerator.getInstance();

    private Map<String,Object> map = null;

    private ESConfiguration76 esConfiguration = null;


    private String esTransPorts;

    private String clusterName;

    private int maxActions;

    public FlinkBulkElasticsearch(String esTransPorts, String clusterName, int maxActions){
        this.esTransPorts = esTransPorts;
        this.clusterName = clusterName;
        this.maxActions = maxActions;
    }

    /**
     * open方法在sink第一次启动时调用，一般用于sink的初始化操作
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        esConfiguration = new ESConfiguration76(esTransPorts, clusterName, maxActions);
    }

    /**
     * invoke方法是sink数据处理逻辑的方法，source端传来的数据都在invoke方法中进行处理
     * 其中invoke方法中第一个参数类型与RichSinkFunction<String>中的泛型对应。第二个参数
     * 为一些上下文信息
     */
    @Override
    public void invoke(BizData data, Context context) throws Exception {

        try {
            if(null==data){
                return;
            }

            int randx = ThreadLocalRandom.current().nextInt(1000,80000000);
            long nextId = idGen.nextId();
            int rdval =  ThreadLocalRandom.current().nextInt(10,8000000);

            String indexId = "pm_" +  data.getKbpNo() + "_" + data.getKpiNo() + "_" + data.getDcTime() +"_"
                    + randx + "_" + nextId + "_"+ rdval+ "_"+System.nanoTime();

            map = new HashMap<>();
            map.put("KBP", data.getKbpNo() + "");
            map.put("KPI_NO", data.getKpiNo() + "");
            map.put("DCTIME", data.getDcTime() + "");
            map.put("VALUE", ConverUtils.Obj2Double(data.getStringValue())+"");
            map.put("GROUPID", "caijinpeng");
            map.put("WRITETIME", System.currentTimeMillis() +"");
            map.put("DRUINGTIME", String.valueOf((System.currentTimeMillis()- data.getReceiveTime())));
            map.put("DRUINGTIME1", String.valueOf((System.currentTimeMillis()- data.getDcTime())));

            ///loggr.info(">>>>>>> save es , indexId="+indexId+", KBP="+data.getKbpNo() +",  KPINO="+ data.getKpiNo()+", DCTIME="+data.getDcTime() );

            /// esConfiguration.bulkAdd(INDEX, INDEX_TYPE, indexId, map);
            esConfiguration.bulkAdd(INDEX, indexId, map);
        } catch (Exception e) {
            loggr.error("bulkProcessor failed ,reason:{}",e);
        }

    }

    /**
     * close方法在sink结束时调用，一般用于资源的回收操作
     */
    @Override
    public void close() throws Exception {
        super.close();
    }

}
