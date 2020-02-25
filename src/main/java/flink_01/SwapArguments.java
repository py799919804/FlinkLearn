package flink_01;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

// 1st element goes into the 2nd position, and 2nd element goes into the 1st position
@FunctionAnnotation.ForwardedFields("0->1; 1->0")
class SwapArguments extends RichMapFunction<Tuple2<Long, Double>, Tuple2<Double, Long>> {
    @Override
    public Tuple2<Double, Long> map(Tuple2<Long, Double> value) {
        // Swap elements in a tuple
        return new Tuple2<>(value.f1, value.f0);
    }
}