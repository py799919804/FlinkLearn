package elasticsearch;

import com.ultrapower.fsms.common.AppEnvironment;
import com.ultrapower.fsms.common.dubbo.RMIUtil;
import com.ultrapower.fsms.common.elasticsearch.util.ESConstans;
import com.ultrapower.fsms.common.sysconfig.RedisConfigFileSubscribe;
import com.ultrapower.fsms.common.utils.ConverUtils;
import com.ultrapower.fsms.common.utils.DateTimeUtil;
import com.ultrapower.fsms.config.kpi.model.MetaKPI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/***
 * 该类实现性能表ES索引的分时处理表维护
 * 可以获取原始索引的当时分时索引信息
 * @author zhaoxr
 *
 */
public class EsIndexDateUtil {
	    private static Logger logger = LoggerFactory.getLogger(EsIndexDateUtil.class);
	    private static EsIndexDateUtil instance =null;
	    public static String DEFAULT_FORMAT ="yyMM";


	    /***
	     * key tablename    :pm_raw_z_z_resinterface
	     * value dateformat :yyyyMM
	     *
	     */
	    private Map<String,String> tablenameCache = new ConcurrentHashMap<String,String>();

	    /***
	     * key   dateformat        :yyyyMM
	     * value dateformat value:201611
	     *
	     */
	    public static ConcurrentHashMap dateValueCache = new ConcurrentHashMap();
	    public static EsIndexDateUtil getInstance(){
	    	if(instance==null) {
          		instance = new EsIndexDateUtil();
        	}
	        return instance;
	    }


	    private EsIndexDateUtil(String appName){
	    	initPmStoreRule();
	    	if(AppEnvironment.isInEnvironment(appName)) {
	    		startDateFormatRegister();
	    	}

	    	EsPmDateConfigFileChange  listener= new EsPmDateConfigFileChange();
	    	RedisConfigFileSubscribe.getInstance().addChangeListener(listener);
	    }



	    /***
//	     * 按本地时间获取ES索引分时表名
	     * @param tableName
	     * @return
	     */
	    public String getEsDateTableName(String tableName){
			// tableName格式为{APPID}_XXX
			String newTableName = tableName;
			int idx = tableName.indexOf("_");
			if (idx > 0) {
				String indexSuffix = tableName.substring(idx + 1);
				String dateFormat = getFormatByTableName(indexSuffix);
				newTableName = tableName + "_" + getEsDateString(dateFormat, 0);
			}
			return newTableName;
		}



	    /***
	     * 按采集时间获取ES索引分时表名
	     * @param tableName
	     * @param dctime
	     * @return
	     */
	    public String getEsDateTableName(String tableName,long dctime){
	    	// tableName格式为{APPID}_XXX
	    	String newTableName = tableName;
	    	int idx = tableName.indexOf("_");
	    	if(idx > 0) {
	    		String indexSuffix = tableName.substring(idx+1);
	    		String dateFormat = getFormatByTableName(indexSuffix);
	    		newTableName = tableName + "_" +  getEsDateString(dateFormat,dctime);
	    	}
	    	return newTableName;
	   }


	/***
	 * 按当前时间，获取最近几个ES索引分时表名
	 * @param tableName
	 * @param preNum
	 * @return
	 */
	public String[] getEsDateTableNames(String tableName, int preNum){
		// 获取近几个月的索引
		List<String> indexNameList = new ArrayList<String>();

		// tableName格式为{APPID}_XXX
		String newTableName = tableName;
		int idx = tableName.indexOf("_");
		if(idx > 0) {
			String indexSuffix = tableName.substring(idx+1);
			String dateFormat = getFormatByTableName(indexSuffix);
			if(preNum==0){
				newTableName = tableName + "_" +  getEsDateString(dateFormat, System.currentTimeMillis());
				indexNameList.add(newTableName);
			}else{
				ESDataHandler esDataHandler = new ESDataHandler();
				for(int i=0; i<preNum; i++) {
					if(i==0) {
						newTableName = tableName + "_" +  getEsDateString(dateFormat, System.currentTimeMillis());
						indexNameList.add(newTableName);
					}else {
						long preMills = System.currentTimeMillis();
						if(dateFormat.contains("yyMMdd") || dateFormat.indexOf("yyMMdd")!=-1){
							preMills = ConverUtils.StrLong2Millis(DateTimeUtil.getNowDataTimeNextDay(-i));
						}else if(dateFormat.contains("yyMM") || dateFormat.indexOf("yyMM")!=-1){
							preMills = ConverUtils.StrLong2Millis(DateTimeUtil.getNowDataTimeNextMonth(-i));
						}else{
							preMills = ConverUtils.StrLong2Millis(DateTimeUtil.getNowDataTimeNextYears(-i));
						}

						String per1_curIndexName = tableName + "_" +  getEsDateString(dateFormat, preMills);
						///String per1_curIndexName = EsIndexDateUtil.getInstance().getEsDateTableName(indexAlias, ConvertUtil.StrLong2Millis(DateTimeUtil.getNowDataTimeNextMonth(-i)));
						// 判断索引类型是否在ES中存在
						boolean rslt = esDataHandler.isExistInDBIndexType(per1_curIndexName, ESConstans.INDEX_TYPE);
						if(rslt) {
							indexNameList.add(per1_curIndexName);
						}else {
							logger.warn("indexName is not exist!!!  indexName:"+per1_curIndexName);
						}

					}
				}
			}
		}

		// 将列表转换为数组
		String[] indexNames = new String[indexNameList.size()];
		indexNameList.toArray(indexNames);
		return indexNames;
	}

	    /***
	     * 索引表分时格式重获取
	     */
	    public void initPmStoreRule()
	    {   logger.info("initPmStoreRule.........");
	    	InputStream in = null;
	    	try {
                byte[] bytes = RMIUtil.getCommonAPI().loadBytesFromServer("conf/es_pm_store_rule.properties" );
                if (bytes != null && bytes.length > 0) {
                    in = new ByteArrayInputStream(bytes);
                }
                Properties sysProp = new Properties();
                if(in!=null) {
                  sysProp.load(in);
                }
    			String tablename = null;
    			if(sysProp.isEmpty())
    			{
    				initDateFormatValue(DEFAULT_FORMAT,System.currentTimeMillis());
    			}
    			else {
    			 for (Enumeration en =sysProp.keys();  en.hasMoreElements();)
                 {
    				 tablename=(String)en.nextElement();
    				 String dateFormatStr= sysProp.getProperty(tablename);
    				 logger.info(tablename +" dateFormatStr :" +dateFormatStr );
    				 tablenameCache.put(tablename.trim().toLowerCase(), dateFormatStr);
    				 initDateFormatValue(dateFormatStr,System.currentTimeMillis());

                 }
    			}

            } catch (Exception ex) {
            	logger.error("get conf/pm-storm-rule.properties from redis catch an exception", ex);
            }
	    }

	    /***
	     * 获取某格式的时间时间串
	     * @param dateFormatStr
	     * @param dctime
	     * @needCache 需要更新
	     * @return
	     */
	    private  String initDateFormatValue(String dateFormatStr,long dctime)
	    {
	    	return initDateFormatValueThread( dateFormatStr, dctime,false);
	    }

	    /***
	     * 获取某格式的时间时间串
	     * @param dateFormatStr
	     * @param dctime
	     * @needCache 需要更新
	     * @return
	     */
	    private  String initDateFormatValueThread(String dateFormatStr,long dctime,boolean needCache)
	    {
	    	 DateFormat dateFormat =null;
			 String dateFormatValue =null;
			 try{
         	   dateFormat = new SimpleDateFormat(dateFormatStr);
         	   dateFormatValue =  dateFormat.format(dctime);

         	   if(DEFAULT_FORMAT.equalsIgnoreCase(dateFormatStr)) {
               dateFormatValue= dateFormatValue+"00";
             }
         	       if(needCache) {
                   dateValueCache.put(dateFormatStr, dateFormatValue);
                 }
         	   }catch(Exception e)
         	   {
         		   logger.warn(dateFormatStr +" format is excepetion :"+e.getMessage() );
         		   //如果配置的格式异常的话，使用默认格式
         		   dateFormat = new SimpleDateFormat(DEFAULT_FORMAT);
           	       dateFormatValue =  dateFormat.format(dctime);
           	       dateFormatValue= dateFormatValue+"00";
           	       if(needCache) {
                     dateValueCache.put(dateFormatStr, dateFormatValue);
                   }
         	   }
			 return dateFormatValue;
	    }

       /***
        * 获取某表的时间范围的查询分区索引
        * @param starttime
        * @param endtime
        * @return
        */
       public List <String> getEsPmIndexesByTimeRange(String tableName ,long starttime ,long endtime ){
		   // tableName格式为{APPID}_XXX
		   String orgTableName = tableName;
		   int idx = tableName.indexOf("_");
		   if (idx > 0) {
			   String indexSuffix = tableName.substring(idx + 1);
			   String dateFormat =getFormatByTableName(indexSuffix);
			   List<String> indexes = DateTimeUtil.getBetween(starttime,endtime,dateFormat);
			   return indexes;
		   }
		   return Stream.of(orgTableName).collect(Collectors.toList());
       }




	    /***
	     * 获取某个性能表名的分时时间格式化字段
	     * @param tableName
	     * @return
	     */
	    private String getFormatByTableName(String tableName){

	    	tableName =tableName.trim().toLowerCase();
			String dateFormat = ConverUtils.Obj2Str(tablenameCache.get(tableName),"");
	    	 if(dateFormat==null ||dateFormat.trim().length()==0 ) {
	    		  dateFormat=DEFAULT_FORMAT;
	    	 }
	         return dateFormat;
	    }


        private String getEsDateString(String dateFormatStr,long dctime){
    	   String timestr= null;
		   if(dctime==0)
		   {
			   timestr= (String)dateValueCache.get(dateFormatStr);
			   if(timestr==null || timestr.trim().length()==0)
			   {

				   timestr =   initDateFormatValue(dateFormatStr,System.currentTimeMillis());

			   }
		   }
		   else{
			   timestr =   initDateFormatValue(dateFormatStr,dctime);
		   }

		   return timestr;

       }

       /**
        * 时间格式注册
        */
       private  void startDateFormatRegister(){

       		logger.info("start  Rmi HeartBeart Register");
           new Thread(){
               @Override
               public void run() {
               	while(true)
                   {

                       try
                       {
                           Thread.sleep (5*1000L);
                           String dateFormatStr =null;
                           for (Enumeration en =dateValueCache.keys()  ;en.hasMoreElements();)
                           {
                        	   dateFormatStr=(String)en.nextElement();
                        	   initDateFormatValueThread(dateFormatStr,System.currentTimeMillis(),true);
                           }

                       }
                       catch(InterruptedException ie)
                       {
                       	 logger.warn ("start Date Format Register Exception ");


                       }
                       catch(RuntimeException nsoe){

                       	logger.warn("start Date Format Register RuntimeException ");


                       }catch(Exception nsoe){

                       	logger.warn("start Date Format Register Exception " );


                       }
                       catch(Throwable ex){

                       	logger.warn("start Date Format Register excetion occures:");
                       }

                   }
               }
           }.start();
       	}

    /***
     * 是否已经存在性能索
     * 对于性能数据，最好在probe端判断下索引表是否已经存在了，存在的话上传，否则不上传
	 * 防止性能索引还没有创建就进行性能数据的插入，导致索引字段类型异常引表
     * @param metaKPI
     * @return
     */
     public boolean hasPmRawTableIndex(MetaKPI metaKPI)
   	{

    	 return true;
   	}

	/**
	 * 获取cm指标索引名
	 * @param tableName
	 * @return
	 */
	public String getCmIndexName(String tableName) {
		// getEsDateTableName(tableName);
		String curIndexName = tableName + "_000000";
		return curIndexName;
	}
}
