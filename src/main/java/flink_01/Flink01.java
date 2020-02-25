package flink_01;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class Flink01 {

    public static void main(String[] args) {

        List l = new ArrayList<String>();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataSource dataSource = env.fromCollection(l);
        DataStream<String> stringDataStream = streamEnv.socketTextStream("", 9999, '\n', 0);
        final SingleOutputStreamOperator<Tuple2<String, String>> map = stringDataStream.map(new IntAdder());
        final SingleOutputStreamOperator<WC> map1 = map.map(a -> {
            return new WC(a.f0, a.f1);
        });
        map1.keyBy(new SelectWord());
    }

}
