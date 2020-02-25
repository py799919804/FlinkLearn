package flink_01;

public class Py {

    private Integer userCode;

    private String userName;

    private String action;

    public int getUserCode() {
        return userCode;
    }

    public String getUserName() {
        return userName;
    }

    public String getAction() {
        return action;
    }

    public void setUserCode(int userCode) {
        this.userCode = userCode;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Py() {
    }

    public Py(Integer userCode, String userName, String action) {
        this.userCode = userCode;
        this.userName = userName;
        this.action = action;
    }
}
