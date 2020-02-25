package flink_01;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class IntAdder implements MapFunction<String,Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> map(String s) throws Exception {
        String[] split = s.split(",");
        return new Tuple2(split[0],split[1]);
    }
}
