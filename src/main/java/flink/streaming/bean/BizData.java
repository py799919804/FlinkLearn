/*
 * BizData.java
 *
 * Created on 2006年4月13日, 下午5:08
 */

package flink.streaming.bean;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * 该部分数据结构经过ID数字化处理后的数据结构
 * 该部分数据可以直接用来入库，也可以从数据库中来构造
 * @author  zgb
 */
public class BizData implements Serializable{
    
    /** Holds value of property kbpNo. */
    private long kbpNo;
    
    /** Holds value of property kpiNo. */
    private long kpiNo;
    
    /** Holds value of property dcTime. */
    private long dcTime;
    
    /** Holds value of property stringValue. */
    private String stringValue;
    
    private String kbp;
    
    // 采集批次号
    private String batchId = null;
    
    private static int round=0;
    private long parentNodeKbpNo; 
    
    private long parentKbpNo ;
    
    //归属主机资源ID
    private long inHostId = 0L;

    private long receiveTime = 0;
    
    static{
    	String pmround=System.getProperty("pm_data_round");
    	if(pmround!=null)
    		round=Integer.parseInt(pmround);
    }
    /**
	 * @return the batchId
	 */
	public String getBatchId() {
		return batchId;
	}
	/**
	 * @param batchId the batchId to set
	 */
	public void setBatchId(String batchId) {
		this.batchId = batchId;
	}
	 public BizData(){
		 
	 }
	/** Creates a new instance of BizData */
    public BizData(long kbpNo, long kpiNo, long dcTime, String value) {
        this.kbpNo = kbpNo;
        this.kpiNo = kpiNo;
        this.dcTime = dcTime;
        if(round>0)
        	value=getRound(value);
        this.stringValue = value;
    }
    
   
    
     public BizData(long kbpNo, long kpiNo, long dcTime, String value,String kbp) {
        this.kbpNo = kbpNo;
        this.kpiNo = kpiNo;
        this.dcTime = dcTime;
        if(round>0)
        	value=getRound(value);
        this.stringValue = value;
        this.kbp = kbp;
    }
    /** Getter for property kbpid.
     * @return Value of property kbpid.
     *
     */
    public long getKbpNo() {
        return this.kbpNo;
    }
    
    /** Setter for property kbpid.
     * @param kbpid New value of property kbpid.
     *
     */
    public void setKbpNo(long kbpNo) {
        this.kbpNo = kbpNo;
    }
    
    /** Getter for property kpino.
     * @return Value of property kpino.
     *
     */
    public long getKpiNo() {
        return this.kpiNo;
    }
    
    /** Setter for property kpino.
     * @param kpino New value of property kpino.
     *
     */
    public void setKpiNo(long kpiNo) {
        this.kpiNo = kpiNo;
    }
    
    /** Getter for property dcTime.
     * @return Value of property dcTime.
     *
     */
    public long getDcTime() {
        return this.dcTime;
    }
    
    /** Setter for property dcTime.
     * @param dcTime New value of property dcTime.
     *
     */
    public void setDcTime(long dcTime) {
        this.dcTime = dcTime;
    }
    
    /** Getter for property stringValue.
     * @return Value of property stringValue.
     *
     */
    public String getStringValue() {
        return this.stringValue;
    }
    
    /** Setter for property stringValue.
     * @param stringValue New value of property stringValue.
     *
     */
    public void setStringValue(String stringValue) {
        if(round>0)
        	stringValue=getRound(stringValue);
        this.stringValue = stringValue;
    }
    
    public String toString(){
        return kbpNo+"-"+kpiNo+"-"+dcTime+"-"+stringValue;
    }
    
    /**
     * 属性 kbp 的获取方法。
     * @return 属性 kbp 的值。
     */
    public String getKbp() {
        return kbp;
    }
    
    /**
     * 属性 kbp 的设置方法。
     * @param kbp 属性 kbp 的新值。
     */
    public void setKbp(String kbp) {
        this.kbp = kbp;
    }
	public long getParentKbpNo() {
		return parentKbpNo;
	}
	public void setParentKbpNo(long parentKbpNo) {
		this.parentKbpNo = parentKbpNo;
	}
	public long getParentNodeKbpNo() {
		return parentNodeKbpNo;
	}
	public void setParentNodeKbpNo(long parentNodeKbpNo) {
		this.parentNodeKbpNo = parentNodeKbpNo;
	}
	
	private String getRound(String value)
	{
		try	{
			if(value.indexOf(".")>-1)
			{
			 BigDecimal b = new BigDecimal(value);
			 BigDecimal one = new BigDecimal("1");
			 String r = b.divide(one, round, BigDecimal.ROUND_HALF_UP).toString();
			 return r;
			}else
			{
				return value ;
			}
		} catch (Exception ex) {
			return value;
		}
	}
    public long getInHostId() {
        return inHostId;
    }
    public void setInHostId(long inHostId) {
        this.inHostId = inHostId;
    }

    public long getReceiveTime() {
        return receiveTime;
    }

    public void setReceiveTime(long receiveTime) {
        this.receiveTime = receiveTime;
    }
}
