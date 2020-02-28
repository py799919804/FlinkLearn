package flink.streaming.common;

import java.io.Serializable;
import java.util.Date;


/**
 * 消息类
 * @author GuoQing LI
 * @see MessageState
 * @version $Id: Message.java,v 1.2 2012/05/18 09:55:41 linj Exp $
 *
 */
public class Message implements Serializable {
    
	private static final long serialVersionUID = 1L;

	/*** 消息系列号自增计数器， 达到Long.MAX_VALUE复位为1 */
    public static  long MESSAGENO_COUNT = 1;
    
    /**消息系列号 */
    private long messageNo = 0;

    /** 消息源主题 */
    private String src = null;

    /** 消息目的主题, topic名称  */
    private String dest = MessageState.NOTIFY_TOPIC;

    /** 消息类型 */
    private long type = -1;

    /** 消息状态 */
    private String state = null;

    /** 消息附加信息 */
    private String addtional = null;

    /** 消息产生时间 */
    private Date upTime = new Date();

    /** 消息用户自定义对象 */
    private Object userObject = null;
    
    /** 消息内容是否压缩 true:压缩 false不压缩 */
    private boolean isCompress = false;

    public Message() {
        src = InetAddressUtil.getLocalIp();
        initMessageNo();
    }
    
    private void initMessageNo(){
        if (MESSAGENO_COUNT >= Long.MAX_VALUE) {
            MESSAGENO_COUNT = 1;
        }
        messageNo = MESSAGENO_COUNT++;
    }

    /**
     * @return
     */
    public String getAddtional() {
        return addtional;
    }

    /**
     * @param addtional
     */
    public void setAddtional(String addtional) {
        this.addtional = addtional;
    }

    /**
     * @return
     */
    public String getDest() {
        return dest;
    }

    /**
     * @param dest
     */
    public void setDest(String dest) {
        this.dest = dest;
    }

    /**
     * @return
     */
    public long getMessageNo() {
        return messageNo;
    }

    /**
     * @param messageNo
     */
    public void setMessageNo(long messageNo) {
        this.messageNo = messageNo;
    }

    /**
     * @return
     */
    public String getSrc() {
        return src;
    }

    /**
     * @param src
     */
    public void setSrc(String src) {
        this.src = src;
    }

    /**
     * @return
     */
    public String getState() {
        return state;
    }

    /**
     * @param state
     */
    public void setState(String state) {
        this.state = state;
    }

    /**
     * @return
     */
    public long getType() {
        return type;
    }

    /**
     * @param type
     */
    public void setType(long type) {
        this.type = type;
    }

    /**
     * @return
     */
    public Date getUpTime() {
        return upTime;
    }

    /**
     * @param upTime
     */
    public void setUpTime(Date upTime) {
        this.upTime = upTime;
    }

    /**
     * @return
     */
    public Object getUserObject() {
        return userObject;
    }

    /**
     * @param userObject
     */
    public void setUserObject(Object userObject) {
        this.userObject = userObject;
    }
    
    public boolean isCompress() {
        return isCompress;
    }

    public void setCompress(boolean isCompress) {
        this.isCompress = isCompress;
    }

    @Override
	public String toString() {
        StringBuffer sb = new StringBuffer(100);
        sb.append(messageNo);
        sb.append(" src: ");
        sb.append(src);
        sb.append(" dest: ");
        sb.append(dest);
        sb.append(" type: ");
        sb.append(type);
   
        return sb.toString();
    }

    public static void main(String[] args) {
        Message m = new Message();
        m.dest = "hello";
        String b = "test";
        System.out.println("test");
        System.out.println("toString: " + b);
    }
}
