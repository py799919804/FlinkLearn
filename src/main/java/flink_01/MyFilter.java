package flink_01;

import org.apache.flink.api.common.functions.FilterFunction;

public class MyFilter implements FilterFunction<Integer> {
    @Override
    public boolean filter(Integer integer) throws Exception {
        return integer !=  0;
    }
}
