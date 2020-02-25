package flink_01;

import org.apache.flink.api.java.functions.KeySelector;

public class SelectWord implements KeySelector<WC, String> {
    @Override
    public String getKey(WC wc) throws Exception {
        return wc.word;
    }
}

