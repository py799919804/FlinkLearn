package flink.streaming.elasticsearch;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ESTest {
	public static void main(String[] args) {
		String esTransPorts = "192.168.172.84:59300,192.168.172.85:59300,192.168.172.86:59300";
		//String esTransPorts = "192.168.182.235:59300,192.168.182.236:59300,192.168.182.237:59300";
		String clusterName = "es760";
		int maxActions = 10000;

		ESConfiguration76 esClient = new ESConfiguration76(esTransPorts, clusterName, maxActions);

		int batch = 100000;
		int batchCnt = 10;
		if (args.length > 0) {
			batch = Integer.parseInt(args[0]);
            batchCnt = Integer.parseInt(args[1]);
		}
		String indexName = "mytest2";
		String typeName = "mytest2";
		Map<String, Object> map = null;
		while(batchCnt > 0) {
			long startTime = System.currentTimeMillis();
			for(int i=  0; i< batch; i++) {
				//每条消息大小为 1KB
				map = new HashMap<>();
				map.put("field1", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field2", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field3", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field4", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field5", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field6", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field7", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field8", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field9", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				map.put("field10", "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
				//ESUtils.add(indexName, typeName, UUID.randomUUID().toString(), map);
				esClient.bulkAdd(indexName, UUID.randomUUID().toString(), map);
			}
			long endTime = System.currentTimeMillis();
			System.out.println("插入 " + batch + " 条数据用时 ： " + (endTime - startTime) + " ms");
			
			//每插入1W条暂停5S
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			batchCnt--;
		}
	}
}
