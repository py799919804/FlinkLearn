package flink.streaming.common;

import java.awt.Color;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/****
 * 转换工具类
 * 
 * 1. 将颜色值不同进制进行相互转换
 * 2. 将object转换为相应字符型、整型
 * 
 * @author caijinpeng 
 *
 */
public class ConverUtils {
	
	//日志
	private static Logger logger = LoggerFactory.getLogger(ConverUtils.class);
	
	/*** =================================================================  
	 * 将颜色值不同进制进行相互转换 
	 * 
	 * ================================================================== **/
	
	/**
	 * 将rgb的颜色值转换为16进制的数值
	 * @param r
	 * @param g
	 * @param b
	 * @return 格式：0xFFFFFF
	 * @throws Exception
	 */
	public static String converColor(int r , int g , int b) throws Exception{
		StringBuffer buffer = new StringBuffer("");
		buffer.append("0x");
		String rs = Integer.toHexString(r);
		if(rs.length()==1){
			buffer.append("0");
		}
		buffer.append(rs);
		String gs = Integer.toHexString(g);
		if(gs.length()==1){
			buffer.append("0");
		}
		buffer.append(gs);
		String bs = Integer.toHexString(b);
		if(bs.length()==1){
			buffer.append("0");
		}
		buffer.append(bs);
		return buffer.toString();
	}

	/**
	 * 通过16进制的字符串值获取颜色对象
	 * @param str
	 * @return
	 */
	public static Color String2Color(String str) {
		int i = Integer.parseInt(str.substring(1), 16);
		return new Color(i);
	}

	/**
	 * 通过颜色对象获取十六进制字符串值
	 * @param color
	 * @return
	 */
	public static String Color2String(Color color) {
		String R = Integer.toHexString(color.getRed());
		R = R.length() < 2 ? ('0' + R) : R;
		String G = Integer.toHexString(color.getGreen());
		G = G.length() < 2 ? ('0' + G) : G;
		String B = Integer.toHexString(color.getBlue());
		B = B.length() < 2 ? ('0' + B) : B;
		return '#' + R + G + B;
	}
	
	
	/*** =================================================================  
	 * 将object转换为相应字符型、整型
	 * 
	 * ================================================================== **/
	/**
	 * 将通用对象s转换为long类型，如果字符穿为空或null，返回r；
	 * @author caijinpeng 
	 * @param s
	 * @param r
	 * @return
	 */
	public static long Obj2long(Object s, long r) {
		long i = r;
		try {
			if(null!=s){
				String str = s.toString().trim();
				i = Long.parseLong(str);	
			}
		} catch (Exception e) {
			i = r;
		}
		return i;
	}
	
	
	/**
	 * 将通用对象s转换为long类型，如果字符穿为空或null，返回0；
	 * @param s
	 * @return
	 * @author caijinpeng 
	 */
	public static long Obj2long(Object s) {
		long i = 0;
		try {
			if(null!=s){
				String str = s.toString().trim();
				i = Long.parseLong(str);
			}
		} catch (Exception e) {
			i = 0;
		}
		return i;
	}
	
	
	/**
	 * 将通用对象s转换为long类型，如果字符穿为空或null，返回r；
	 * @param s
	 * @return
	 * @author caijinpeng 
	 */
	public static double Obj2Double(Object s, double r) {
		double i = r;
		try {
			if(null!=s){
				String str = s.toString().trim();
				i = Double.parseDouble(str);
			}
		} catch (Exception e) {
			i = r;
		}
		return i;
	}
	
	/**
	 * 将通用对象s转换为long类型，如果字符穿为空或null，返回0；
	 * @param s
	 * @return
	 * @author caijinpeng 
	 */
	public static double Obj2Double(Object s) {
		double i = 0;
		try {
			if(null!=s){
				String str = s.toString().trim();
				i = Double.parseDouble(str);
			}
		} catch (Exception e) {
			i = 0;
		}
		return i;
	}
	
	
	/**
	 * 将通用对象s转换为int类型，如果字符穿为空或null，返回r；
	 * @author caijinpeng 
	 * @param s
	 * @param r
	 * @return
	 */
	public static int Obj2int(Object s, int r) {
		int i = r;
		try {
			if(null!=s){
				String str = s.toString().trim();
				i = Integer.parseInt(str);
			}
		} catch (Exception e) {
			i = r;
		}
		return i;
	}
	
	/**
	 * 将通用对象s转换为int类型，如果字符穿为空或null，返回0；
	 * @param s
	 * @return
	 * @author caijinpeng 
	 */
	public static int Obj2int(Object s) {
		int i = 0;
		try {
			if(null!=s){
				String str = s.toString().trim();
				i = Integer.parseInt(str);
			}
		} catch (Exception e) {
			i = 0;
		}
		return i;
	}
	
	
	/**
	 * 将通用对象s转换为float类型，如果字符穿为空或null，返回r；
	 * @author caijinpeng 
	 * @param s
	 * @param r
	 * @return
	 */
	public static float Obj2Float(Object s, float r) {
		float i = r;
		try {
			if(null!=s){
				String str = s.toString().trim();
				i = Float.parseFloat(str);
			}
		} catch (Exception e) {
			i = r;
		}
		return i;
	}
	
	/**
	 * 将通用对象s转换为float类型，如果字符穿为空或null，返回0；
	 * @param s
	 * @return
	 * @author caijinpeng 
	 */
	public static float Obj2Float(Object s) {
		float i = 0;
		try {
			if(null!=s){
				String str = s.toString().trim();
				i = Float.parseFloat(str);
			}
		} catch (Exception e) {
			i = 0;
		}
		return i;
	}

	/**
	 * 将通用对象s转换为String类型，如果字符穿为空或null，返回r；
	 * @author caijinpeng 
	 * @param s
	 * @param r
	 * @return
	 */
	public static String Obj2Str(Object s, String r) {
		String str = r;
		try {
			if(null!=s){
				str = s.toString().trim();
			}
			
			if(str.equalsIgnoreCase("null") || str.trim().length() == 0){
				str = r;
			}
		} catch (Exception e) {
			str = r;
		}
		return str;
	}
	
	
	/**
	 * 将字符串 转换成 boolean 类型
	 * @param r
	 * @return
	 */
	public static boolean Str2Boolean(String r){
	    boolean result = false;
        try {
            if(null!=r && r.trim().length()>0){
                Boolean rBoolean = Boolean.parseBoolean(r.trim());
                if(null!=rBoolean){
                    result = rBoolean.booleanValue();
                }
            }
        } catch (Exception e) {
            result = false;
        }
        return result;
	}
	
	
	/**
	 * 去掉字符串 中前后的空格
	 * @param r
	 * @return
	 */
	public static String StringTrim(String r){
		String str = r;
		try{
			if(null!=r){
				str = r.trim();
			}
		}catch(Exception e){
			str = r;
		}
		return str;
	}
	
	
	/**
	 * 将long对象s转换为String类型，如果s为0，返回字符串r；
	 * @param s
	 * @return
	 */
	public static String Long2Str(long s, String r){
		String str = r;
		try {
			if(s!=0){
				str = String.valueOf(s);
			}
		} catch (Exception e){
			str = r;
		}
		return str;
	}
	
	
	/***
	 * 将double字符串对象d, 转换为int类型，如果d为NULL和空，返回0；
	 * @param d
	 * @return
	 */
	public static int DoubleStr2Int(String d){
		int i = 0;
		try{
			if(d!=null && d.trim().length()>0){
				Double D1=new Double(d);    
				i = D1.intValue();
			}
		}catch(Exception e){
			i = 0;
		}
		return i;
	}
	
	/***
	 * 将double字符串对象d, 转换为long类型，如果d为NULL和空，返回0；
	 * @param d
	 * @return
	 */
	public static long DoubleStr2Long(String d){
		long i = 0;
		try{
			if(d!=null && d.trim().length()>0){
				Double D1=new Double(d);    
				i = D1.longValue();
			}
		}catch(Exception e){
			i = 0;
		}
		return i;
	}
	
	
	/*** =================================================================  
	 * 时间不同格式进行转换
	 * 
	 * ================================================================== **/
	/**
	 * 将毫秒数转换为yyyy-MM-dd HH:mm:ss格式的时间串
	 * @param millis
	 * @return
	 */
	public static String Millis2StrLong(long millis){
		if (millis <= 0){
			return "";
		}
		
		try{
			DateTimeFormatter df= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			Instant instant = Instant.ofEpochMilli(millis);
			LocalDateTime localDeteTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
			String s = df.format(localDeteTime);
			return s;
		}catch(Exception ex){
			logger.error("Millis2StrLong error! request millis:"+millis, ex);
		}
		return "";
	}
	
	
	/**
	 * 将毫秒数转换为HH:mm:ss格式的时间串
	 * @param millis
	 * @return
	 */
	public static String Millis2HmsStrLong(long millis){
		if (millis <= 0) {
			return "";
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(millis);
		Date date = calendar.getTime();
		SimpleDateFormat datetimeFormat = new SimpleDateFormat("HH:mm:ss");
		String s = datetimeFormat.format(date);
		return s;
	}
	
	
	/**
     * 将系统当前时间转换为特定格式的时间串返回
     * 
     * @param _dtFormat 格式 例如：yyyy-MM-dd HH:mm:ss 或者 yyyy-MM-dd 或者 yyyyMMddhh 等等；
     * @return
     */
    public static String getCurrentTimeByFormat(String _dtFormat){
        String s = "";
        if(_dtFormat == null || _dtFormat.trim().equals("")){
            _dtFormat = "yyyy-MM-dd HH:mm:ss";
        }
        
        try {
            Calendar calendar = Calendar.getInstance();
            Date date = calendar.getTime();
            SimpleDateFormat datetimeFormat = new SimpleDateFormat(_dtFormat);
            s = datetimeFormat.format(date);
        } catch (Exception e) {
            logger.error("getCurrentTimeByFormat exception, _dtFormat=" + _dtFormat, e);
        }
        
        return s;
    }
    
    /**
     * 将传入long型时间转换为特定格式的时间串返回
     * 
     * @param millis    传入long型时间
     * @param _dtFormat 格式 例如：yyyy-MM-dd HH:mm:ss 或者 yyyy-MM-dd 或者 yyyyMMddhh 等等；
     * @return
     */
    public static String getCurrentTimeByFormat(long millis, String _dtFormat){
        String s = "";
        if(_dtFormat == null || _dtFormat.trim().equals("")){
            _dtFormat = "yyyy-MM-dd HH:mm:ss";
        }
        
        if (millis <= 0)
            return "";
        
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(millis);
            Date date = calendar.getTime();
            SimpleDateFormat datetimeFormat = new SimpleDateFormat(_dtFormat);
            s = datetimeFormat.format(date);
        } catch (Exception e) {
            logger.error("getCurrentTimeByFormat exception, _dtFormat=" + _dtFormat, e);
        }
        
        return s;
    }
    
    
	
	/**
	 * 将yyyy-MM-dd HH:mm:ss类型的字符串转换为毫秒数
	 * @param dateStr
	 * @return
	 */
	public static long StrLong2Millis(String dateStr){
		if (dateStr == null || "".equals(dateStr.trim())) {
			return 0;
		}
		
		try{
			DateTimeFormatter df= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			LocalDateTime localDateTime = LocalDateTime.parse(dateStr, df); 
			Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
			long lTime = instant.toEpochMilli();
			return lTime;
		}catch(Exception ex){
			logger.error("StrLong2Millis error! request dateStr:"+dateStr, ex);
		}
		return 0;
	}
	
	
	
	/**
	 * 将毫秒数转换为yyyy-MM-dd格式的时间串
	 * @param millis
	 * @return
	 */
	public static String Millis2Str(long millis){
		if (millis <= 0){
			return "";
		}
		
		try{
			DateTimeFormatter df= DateTimeFormatter.ofPattern("yyyy-MM-dd");
			Instant instant = Instant.ofEpochMilli(millis);
			LocalDateTime localDeteTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
			String s = df.format(localDeteTime);
			return s;
		}catch(Exception ex){
			logger.error("Millis2Str error! request millis:"+millis, ex);
		}
		return "";
	}
	
	
	/**
	 * 将yyyy-MM-dd类型的字符串转换为毫秒数
	 * @param dateStr
	 * @return
	 */
	public static long Str2Millis(String dateStr){
		if (dateStr == null || "".equals(dateStr.trim())) {
			return 0;
		}
		
		try{
			DateTimeFormatter df= DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			LocalDateTime localDateTime = LocalDateTime.parse(dateStr, df); 
			Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
			long lTime = instant.toEpochMilli();
			return lTime;
		}catch(Exception ex){
			logger.error("Str2Millis error! request dateStr:"+dateStr, ex);
		}
		return 0;
	}
	

	/**
     * 将给定类型的时间字符串转换为毫秒数
     * @param dateStr
     * @param _dtFormat
     *     日期格式1  yyyy-MM-dd HH:mm:ss
     *     日期格式2  yyyy-MM-dd
     *     日期格式3  yyyyMMddHH
     * @return
     */ 
    public static long StrFormat2Millis(String dateStr, String _dtFormat){
    	if (dateStr == null || "".equals(dateStr.trim())) {
			return 0;
		}
		
		try{
			DateTimeFormatter df= DateTimeFormatter.ofPattern(_dtFormat);
			LocalDateTime localDateTime = LocalDateTime.parse(dateStr, df); 
			Instant instant = localDateTime.atZone(ZoneId.systemDefault()).toInstant();
			long lTime = instant.toEpochMilli();
			return lTime;
		}catch(Exception ex){
			logger.error("StrFormat2Millis error! request dateStr:"+dateStr+", _dtFormat:"+_dtFormat, ex);
		}
		return 0;      
    }
    
    
    
    
    /**
     * 将给定类型的时间字符串转换为时间
     * @param dateStr
     * @param _dtFormat
     *     日期格式1  yyyy-MM-dd HH:mm:ss
     *     日期格式2  yyyy-MM-dd
     *     日期格式3  yyyyMMddHH
     * @return
     */ 
    public static Date StrFormat2Date(String dateStr, String _dtFormat){
		try{
			DateTimeFormatter df = DateTimeFormatter.ofPattern(_dtFormat);
			LocalDateTime ldt = LocalDateTime.parse(dateStr, df);
			
			ZoneId zoneId = ZoneId.systemDefault();
			ZonedDateTime zdt = ldt.atZone(zoneId);
	        Date date = Date.from(zdt.toInstant());
			return date;
		}catch(Exception ex){
			logger.error("StrFormat2Date error! request dateStr:"+dateStr+", _dtFormat:"+_dtFormat, ex);
		}
		return null;
    }
    
    
    /**
     * 将13位时间毫秒数转换为 指定格式的时间字符串串
     * @param millis
     * @param _dtFormat
     *     日期格式1  yyyy-MM-dd HH:mm:ss
     *     日期格式2  yyyyMMdd
     *     日期格式3  yyyyMMddHH
     * @return
     */
    public static String Millis2FormatStr(long millis, String _dtFormat){
        if (millis <= 0){
			return "";
		}
		
		try{
			DateTimeFormatter df= DateTimeFormatter.ofPattern(_dtFormat);
			Instant instant = Instant.ofEpochMilli(millis);
			LocalDateTime localDeteTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
			String s = df.format(localDeteTime);
			return s;
		}catch(Exception ex){
			logger.error("Millis2FormatStr error! request millis:"+millis+", _dtFormat:"+_dtFormat, ex);
		}
		return "";
    }
    
    
	 /***
	  * 将指定时间字符串和时间格式，延后或前移几分钟的时间
	  * @param dateStr   时间字符串
	  * @param _dtFormat 时间格式  yyyy-MM-dd
	  *                        yyyy-MM-dd HH:mm:ss
	  * @param mins      分钟
	  * @return
	  * @author caijinpeng
	  */
	 public static String getDataTimeNextTime(String dateStr, String _dtFormat, int mins) {
		 String plusDateTimeStr = ""; 
		 try{
			  DateTimeFormatter df = DateTimeFormatter.ofPattern(_dtFormat);
			  LocalDateTime ldt = LocalDateTime.parse(dateStr, df);
			  LocalDateTime date = ldt.minusMinutes(mins);
			  plusDateTimeStr = date.format(df); 
		 }catch(Exception e){
			  return plusDateTimeStr;
		 }
		 return plusDateTimeStr;
	 }
	 
	 /***
	  * 将指定时间字符串和时间格式，延后或前移几天的时间
	  * @param dateStr   时间字符串
	  * @param _dtFormat 时间格式  yyyy-MM-dd
	  *                        yyyy-MM-dd HH:mm:ss
	  * @param days      天数
	  * @return
	  * @author caijinpeng
	  */
	 public static String getDataNextDay(String dateStr, String _dtFormat, int days) {
		 String plusDateTimeStr = ""; 
		 try{
			  DateTimeFormatter df = DateTimeFormatter.ofPattern(_dtFormat);
			  LocalDateTime ldt = LocalDateTime.parse(dateStr, df);
			  LocalDateTime date = ldt.plusDays(days);
			  plusDateTimeStr = date.format(df); 
		 }catch(Exception e){
			  return plusDateTimeStr;
		 }
		 return plusDateTimeStr;
	 }
	 
	 
	 
	 /**
	  * 获取当前时间，延后或前移几小时的时间
	  * @param hours 小时
	  * @return
	  */
	 public static String getNowDataTimeNextHour(int hours) {
		 String plusDateTimeStr = "";
		 try {
			LocalDateTime date = LocalDateTime.now().plusHours(hours);
			DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			plusDateTimeStr = date.format(f); 
		 } catch (Exception ex) {
			logger.error("getNowDataTimeNextHour error! request hours:"+hours, ex);
		 }
		 return plusDateTimeStr;
	 }
	 
	 
	 
	 /**
	  * 获取当前时间，延后或前移几天的时间
	  * @param days 天数
	  * @return
	  */
	 public static String getNowDataTimeNextDay(int days) {
		 String plusDateTimeStr = "";
		 try {
			LocalDateTime date = LocalDateTime.now().plusDays(days);
			DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			plusDateTimeStr = date.format(f); 
		 } catch (Exception ex) {
			logger.error("getNowDataTimeNextDay error! request days:"+days, ex);
		 }
		 return plusDateTimeStr;
	 }
	 
	 /**
	  * 获取当前时间，延后或前移几个月时间
	  * @param month 月
	  * @return
	  */
	 public static String getNowDataTimeNextMonth(int month) {
		 String plusDateTimeStr = "";
		 try {
			LocalDateTime date = LocalDateTime.now().plusMonths(month);
			DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			plusDateTimeStr = date.format(f); 
		 } catch (Exception ex) {
			logger.error("getNowDataTimeNextMonth error! request month:"+month, ex);
		 }
		 return plusDateTimeStr;
	 }
	 
	 
	 /**
	  * 获取当前时间，延后或前移几个年时间
	  * @param years 年
	  * @return
	  */
	 public static String getNowDataTimeNextYears(int years) {
		 String plusDateTimeStr = "";
		 try {
			LocalDateTime date = LocalDateTime.now().plusYears(years);
			DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			plusDateTimeStr = date.format(f); 
		 } catch (Exception ex) {
			logger.error("getNowDataTimeNextYears error! request years:"+years, ex);
		 }
		 return plusDateTimeStr;
	 }
	 
	 
	 /**
	  * 将时间字符串yyyy-MM-dd HH:mm:ss, 转换为yyyy-MM-dd HH:mm:00
	  * @param datastr
	  * @return
	  */
	 public static String DateTimeStr2YMDHM0(String datastr){
		 if(null==datastr || datastr.trim().length()==0){
			 return datastr;
		 }
		 
		 SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		 String mydate1 = "";
		 try {
			   Date date1 = format.parse(datastr);

			   SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
			   mydate1 = datetimeFormat.format(date1);
			   mydate1 +=":00";
		 } catch (Exception e) {
		 }
		 return mydate1;
	 }
	 
    
	 
	 /**
	  * 将短时间格式字符串转换为时间 yyyy-MM-dd
	  *
	  * @param strDate
	  * @return
	  */
	 public static Date strToDate(String strDate) {
		 try {
			 DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd");
			 LocalDateTime ldt = LocalDateTime.parse(strDate, df);
			
			 ZoneId zoneId = ZoneId.systemDefault();
			 ZonedDateTime zdt = ldt.atZone(zoneId);
			 Date date = Date.from(zdt.toInstant());
			 return date;
		 }catch(Exception ex) {
			 ex.printStackTrace();
		 }
		 return null;
	 }
	 
	 
    /** 
     * 按照日前模式和本地化转化日期为字符串
     * @param pattern
     * @param locale
     * @param date
     *
     */
    public static String getDateString(String pattern, Locale locale, Date date) {
        try {
            SimpleDateFormat df = new SimpleDateFormat(pattern, locale);
            
            return df.format(date);
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
    
    /**
     * 将时间秒转换为时分秒(HH时mm分ss秒)
     * @param time
     * @return
	 *    1. 30秒
	 *    2. 2分40秒   
	 *    3. 4小时50分5秒
     */
    public static String secondToHMSTime(int time) {  
        String timeStr = null;  
        int hour = 0;  
        int minute = 0;  
        int second = 0;  
        if (time <= 0) {
          return "0分0秒";
        } else {
            minute = time / 60;  
            if (minute < 60) {  
                second = time % 60;  
                timeStr = minute + "分" + unitFormat(second)+"秒";  
            } else {  
                hour = minute / 60;  
                if (hour > 99999) {
                    return "99999时59分59秒"; 
                }
                minute = minute % 60;  
                second = time - hour * 3600 - minute * 60;  
                timeStr = hour + "时" + unitFormat(minute) + "分" + unitFormat(second)+"秒";  
            }  
        }  
        return timeStr;  
    } 
    
    
    
    private static String unitFormat(int i) {  
        String retStr = null;  
        if (i >= 0 && i < 10) { 
            retStr = "0" + Integer.toString(i);  
        }else{  
            retStr = "" + i;  
        }
        return retStr;  
    }  
 
    

    /**
     * 将毫秒值转换为时分秒(HH时mm分ss秒)
     * @param mtime
     * @return
	 *    1. 30秒
	 *    2. 2分40秒   
	 *    3. 4小时50分5秒
     */
	public static String Millis2HMSTime(long mtime){
		long ltime = mtime / 1000;
		int time = Integer.parseInt(ltime+"");
		
		String timeStr = null;  
        int hour = 0;  
        int minute = 0;  
        int second = 0;  
        if (time <= 0) {
          return "0分0秒";
        } else {
            minute = time / 60;  
            if (minute < 60) {  
                second = time % 60;  
                if(minute==0){
                     timeStr = unitFormat(second)+"秒";  
                }else{
                	timeStr = minute + "分" + unitFormat(second)+"秒";  
                }
            } else {  
                hour = minute / 60;  
                if (hour > 99999)  {
                    return "99999时59分59秒"; 
                }
                minute = minute % 60;  
                second = time - hour * 3600 - minute * 60;  
                timeStr = hour + "时" + unitFormat(minute) + "分" + unitFormat(second)+"秒";  
            }  
        }  
        return timeStr;  
	}
	
	/* 
	 * 将毫秒值转换为时分秒(D天HH小时mm分钟)
     * 将 毫秒转化   天  小时  分钟 
     * @param ms 毫秒数  
     */  
    public static String Millis2DHMTime(Long ms) {    
        Integer ss = 1000;    
        Integer mi = ss * 60;    
        Integer hh = mi * 60;    
        Integer dd = hh * 24;    
        
        Long day = ms / dd;    
        Long hour = (ms - day * dd) / hh;    
        Long minute = (ms - day * dd - hour * hh) / mi;    
        Long second = (ms - day * dd - hour * hh - minute * mi) / ss;    
            
        StringBuilder sb = new StringBuilder();    
        if(day > 0) {    
            sb.append(day+"天");    
        }    
        if(hour > 0) {    
            sb.append(hour+"小时");    
        }    
        if(minute > 0) {    
            sb.append(minute+"分钟");    
        }    
        if(second > 0) {    
            sb.append(second+"秒");    
        }    
        return sb.toString();    
    }  
	
	
	/* 
	 * 将秒值转换为时分秒(D天HH小时mm分钟)
     * 将 毫秒转化   天  小时  分钟 
     * @param ms 秒数  
     */  
    public static String secondMillis2DHMTime(Long s) {    
        Integer ss = 1;    
        Integer mi = ss * 60;    
        Integer hh = mi * 60;    
        Integer dd = hh * 24;    
        
        Long day = s / dd;    
        Long hour = (s - day * dd) / hh;    
        Long minute = (s - day * dd - hour * hh) / mi;   
		Long second = (s - day * dd - hour * hh - minute * mi) / ss;    
            
        StringBuilder sb = new StringBuilder();    
        if(day > 0) {    
            sb.append(day+"天");    
        }    
        if(hour > 0) {    
            sb.append(hour+"小时");    
        }    
        if(minute > 0) {    
            sb.append(minute+"分钟");    
        }    
        if(second > 0) {    
            sb.append(second+"秒");    
        }  
        return sb.toString();    
    }




	/**
	 * 把list字符串，拆分成多个批次
	 * @param list 集合
	 * @param batchSize 批次大小
	 * @return Map<Integer,List<Long>>
	 */
	public static Map<Integer, List<String>> ListStrSplit2BatchList(List<String> list, int batchSize){
		Map<Integer,List<String>> itemMap = new HashMap<>();
		itemMap.put(1, new ArrayList<String>());
		for(String e : list){
			List<String> batchList= itemMap.get(itemMap.size());
			if(batchList.size() == batchSize){//当list满足批次数量，新建一个list存放后面的数据
				batchList = new ArrayList<String>();
				itemMap.put(itemMap.size()+1, batchList);
			}
			batchList.add(e);
		}
		return itemMap;
	}


	/**
	 * 把list数值，拆分成多个批次
	 * @param list 集合
	 * @param batchSize 批次大小
	 * @return Map<Integer,List<String>>
	 */
	public static Map<Integer, List<Long>> ListNumSplit2BatchList(List<Long> list, int batchSize){
		Map<Integer,List<Long>> itemMap = new HashMap<>();
		itemMap.put(1, new ArrayList<Long>());
		for(Long e : list){
			List<Long> batchList= itemMap.get(itemMap.size());
			if(batchList.size() == batchSize){//当list满足批次数量，新建一个list存放后面的数据
				batchList = new ArrayList<Long>();
				itemMap.put(itemMap.size()+1, batchList);
			}
			batchList.add(e);
		}
		return itemMap;
	}


	/**
	 * 获得当前 [给定日期时间 格式] 的字符串
	 *
	 * @param _dtFormat
	 *     日期格式1  yyyy-MM-dd HH:mm:ss
	 *     日期格式2  yyyy-MM-dd
	 * @return 给定日期格式的字符串
	 */
	public static String getNowDateTime(String _dtFormat) {
		String currentdatetime = "";
		try {
			Date date = new Date(System.currentTimeMillis());
			SimpleDateFormat dtFormat = new SimpleDateFormat(_dtFormat);
			currentdatetime = dtFormat.format(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return currentdatetime;
	}


	/**
	 * 获得当前日期时间格式的字符串
	 *
	 * @param
	 * @return 当前日期时间格式的字符串 yyyy-MM-dd HH:mm:ss
	 */
	public static String getNewDateTime() {
		return getNowDateTime("yyyy-MM-dd HH:mm:ss");
	}

    
    /**
     * 将秒转换“天时分秒”
     * @param mss
     * @return
     */
    public static String secs2DayHourMinSecs(long mss) {
    	String DateTimes = null;
    	long days = mss / ( 60 * 60 * 24);
    	long hours = (mss % ( 60 * 60 * 24)) / (60 * 60);
    	long minutes = (mss % ( 60 * 60)) /60;
    	long seconds = mss % 60;
    	if(days>0){
    	   DateTimes= days + "天" + hours + "小时" + minutes + "分钟"
    	     + seconds + "秒"; 
    	}else if(hours>0){
    	   DateTimes=hours + "小时" + minutes + "分钟"
    	     + seconds + "秒"; 
    	}else if(minutes>0){
    	   DateTimes=minutes + "分钟"
    	     + seconds + "秒"; 
    	}else{
    	   DateTimes=seconds + "秒";
    	}
    	  
    	return DateTimes;
    }
    
    
    /**
     * 根据millis获取当天00:00:00的13毫秒时间
     * 
     * @author liwei 
     * @create time：2016年11月30日  下午1:10:24
     * @param millis
     * @return
     */
    public static long getZeroTimeByMillis(long millis){
        long l_time = 0L;
        if (millis <= 0)
            return l_time;
        
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(millis);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            l_time = calendar.getTimeInMillis();
        } catch (Exception e) {
        	logger.error("getZeroTimeByMillis exception, millis=" + millis, e);
        }
        
        return l_time;
    }
    
    /**
     * 根据millis获取当月1号00:00:00的13毫秒时间
     * 
     * @author liwei 
     * @create time：2016年11月30日  下午1:10:24
     * @param millis
     * @return
     */
    public static long getMonthByMillis(long millis){
        long l_time = 0L;
        if (millis <= 0)
            return l_time;
        
        try {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(millis);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            l_time = calendar.getTimeInMillis();
        } catch (Exception e) {
        	logger.error("getMonthByMillis exception, millis=" + millis, e);
        }
        
        return l_time;
    }
    
    public static void main(String[] args){
        System.out.println(getMonthByMillis(System.currentTimeMillis()));
    }
 
}
