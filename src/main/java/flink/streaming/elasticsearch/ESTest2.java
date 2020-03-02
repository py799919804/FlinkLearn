//package flink.streaming.elasticsearch;
//
//import com.ultrapower.fsms.flink.streaming.bean.BizData;
//import com.ultrapower.fsms.flink.streaming.common.ConverUtils;
//
//import java.util.Date;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.UUID;
//import java.util.concurrent.TimeUnit;
//
//public class ESTest2 {
//    public static void main(String[] args) {
//        String esTransPorts = "192.168.172.84:59300,192.168.172.85:59300,192.168.172.86:59300";
//        //String esTransPorts = "192.168.182.235:59300,192.168.182.236:59300,192.168.182.237:59300";
//        String clusterName = "es760";
//        int maxActions = 10000;
//        String indexName = "mytest3";
//        long batchCnt = 50;
//        long batchSize = 100000;
//
//        if (args.length == 5) {
//            esTransPorts = args[0];
//            clusterName = args[1];
//            indexName  = args[2];
//            batchCnt = Integer.parseInt(args[3]);
//            batchSize = Integer.parseInt(args[4]);
//        }
//
//        ESConfiguration76 esClient = new ESConfiguration76(esTransPorts, clusterName, maxActions);
//        Map<String, Object> map = null;
//        /**
//         * "GROUPID": "caijinpeng",
//         * "KBP": "2067327",
//         * "KPI_NO": "2016112300008",
//         * "DRUINGTIME": "0",
//         * "VALUE": "99.0",
//         * "DCTIME": "1582272197318",
//         * "DRUINGTIME1": "464732",
//         * "WRITETIME": "1582272662050"
//         */
//
//        long startTime = System.currentTimeMillis();
//        for (int curBatch = 1; curBatch <= batchCnt; curBatch++) {
//            long batchStartTime = System.currentTimeMillis();
//            for (int i = 0; i < batchSize; i++) {
//                //每条消息大小为 1KB
//                map = new HashMap<>();
//                map.put("KBP", 2067327L);
//                map.put("KPI_NO", 2016112300008L);
//                map.put("DCTIME", 1582272197318L);
//                map.put("VALUE", 100d);
//                map.put("GROUPID", "caijinpeng");
//                map.put("WRITETIME", System.currentTimeMillis());
//                map.put("DRUINGTIME", String.valueOf((System.currentTimeMillis() - 1582272197318L)));
//                map.put("DRUINGTIME1", String.valueOf((System.currentTimeMillis() - 1582272197318L)));
//
//                //ESUtils.add(indexName, typeName, UUID.randomUUID().toString(), map);
//                esClient.bulkAdd(indexName, UUID.randomUUID().toString(), map);
//            }
//            long totalCntW = curBatch * batchSize / 10000;
//            long endTime = System.currentTimeMillis();
//            System.out.println("\n插入 " + batchSize + " 条数据用时 ： " + (endTime - batchStartTime) + " ms");
//            float speed = (totalCntW * 1f) / ((endTime - startTime) / 1000f);
//            System.out.println("当前时间 ： " + new Date().toLocaleString() + ", 已插入数据量 ： " + totalCntW + "  W条，平均插入性能：" + String.format("%.2f", speed) + " W条每秒");
//
//            //每插入1W条打印一次并暂停5S
//			/*try {
//				TimeUnit.SECONDS.sleep(5);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}*/
//        }
//    }
//}
