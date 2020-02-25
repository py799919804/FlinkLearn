//package flink_01;
//
//
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.cep.pattern.conditions.IterativeCondition;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class FlinkCEPTest {
//
//    public static void main(String[] args) {
//        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        DataStream<String> dataStream = streamEnv.socketTextStream("localhost", 9999);
//        SingleOutputStreamOperator<Py> pyInput = dataStream.map(new RichMapFunction<String, Py>() {
//
//            @Override
//            public Py map(String s) throws Exception {
//                String[] split = s.split(",");
//                Py py = new Py(Integer.parseInt(split[0]), split[1], split[2]);
//                return py;
//            }
//        });
//
//        final Pattern<Object, Object> start = Pattern.begin("start").where(new IterativeCondition<String>() {
//
//            @Override
//            public boolean filter(String value, Context<String> ctx) throws Exception {
//                return false;
//            }
//        })
//
//    }
//}
