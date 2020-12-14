package elasticsearch;

import com.ultrapower.fsms.config.kpi.model.MetaKPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;


/***
 * 该类实现性能表ES索引的是否存在判断
 * @author zhaoxr
 *
 */
public class EsIndexManUtil {
	    private static Logger logger = LoggerFactory.getLogger(EsIndexManUtil.class);
	    private static EsIndexManUtil instance =null;
	    public static boolean needESDateTableFlag=true;
	    /***
	     * key : datasource +"-" + classtitle 
	     */
	    private ConcurrentHashMap tablenameIndexCache = new ConcurrentHashMap();
	
	    public static EsIndexManUtil getInstance(){
	    	if(instance==null) {
          instance = new EsIndexManUtil();
        }
	        return instance;
	    }

	    
	    private EsIndexManUtil(){
	    	 

	    }
	    

    /***
     * 是否已经存在性能索
     * 对于性能数据，最好在probe端判断下索引表是否已经存在了，存在的话上传，否则不上传
	 * 防止性能索引还没有创建就进行性能数据的插入，导致索引字段类型异常引表  
     * @param metaKPI
     * @return
     */
     public boolean hasPmRawTableIndex(MetaKPI metaKPI)
   	{    boolean flag=true;
//   	     String es_index  = RawDataTableUtil.getTableName(metaKPI);
//   	     
//   	     es_index = es_index.toLowerCase();
//   	    
//    	 if(tablenameIndexCache.containsKey(es_index.toLowerCase()))
//    	 {
//    		 flag=true; 
//    	 }else
//    	 {
//    		 try {
//				boolean Indexflag = RMIUtil.getBizManAPI().hasIndexInRedis(es_index);
//				if(Indexflag)
//				{
//					tablenameIndexCache.put(es_index, "");
//					flag = true ;
//				}else
//				{
//					 logger.warn("---es index not has------" + es_index);
//				}
//			} catch (RemoteException e) {
//				logger.warn("do  hasIndexInRedis : "+ es_index + ",Exception : "+ e.getMessage());
//				}
//    	
//    	 }
//   		
    	 return flag;
   	}

}
