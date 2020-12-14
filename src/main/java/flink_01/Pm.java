package flink_01;

public class Pm {

    private String host;
    private String type;
    private double value;

    public Pm() {
    }

    public Pm(String host, String type, double value) {
        this.host = host;
        this.type = type;
        this.value = value;
    }

    public String getHost() {
        return host;
    }

    public String getType() {
        return type;
    }

    public double getValue() {
        return value;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
