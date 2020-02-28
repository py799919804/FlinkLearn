package flink.streaming.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @Title:
 *
 * @description:
 * 
 * @Company: ultrapower.com
 * @author yanglh
 * @create time：May 24, 2016 5:48:01 PM
 * @version 1.0
 */
public class UploadBizData implements Serializable {

    private static final long serialVersionUID = -2715866291270248262L;

    private List<BizData> datas = null;

    private long groupingTime = 0;

    private String groupName = null;

    private long kbpID = -1;

    private long stormReceiveTime = 0;

    private String type = null;

    private long uploadTime = 0;

    public void addData(BizData data) {
        if (datas == null) {
            datas = new ArrayList<BizData>();
        }

        datas.add(data);
    }

    public List<BizData> getDatas() {
        return datas;
    }

    public long getGroupingTime() {
        return groupingTime;
    }

    public String getGroupName() {
        return groupName;
    }

    public long getKbpID() {
        return kbpID;
    }
    
    public long getStormReceiveTime() {
        return stormReceiveTime;
    }
    public String getType() {
        return type;
    }
    public long getUploadTime() {
        return uploadTime;
    }

    public void removeAll() {
        if (datas != null) {
            datas.clear();
        }
    }

    public void setDatas(List<BizData> datas) {
        this.datas = datas;
    }

    public void setGroupingTime(long groupingTime) {
        this.groupingTime = groupingTime;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public void setKbpID(long kbpID) {
        this.kbpID = kbpID;
    }

    public void setStormReceiveTime(long stormReceiveTime) {
        this.stormReceiveTime = stormReceiveTime;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setUploadTime(long uploadTime) {
        this.uploadTime = uploadTime;
    }

}

