//package flink_01;
//
//
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.cep.CEP;
//import org.apache.flink.cep.PatternStream;
//import org.apache.flink.cep.RichPatternSelectFunction;
//import org.apache.flink.cep.nfa.aftermatch.*;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.cep.pattern.conditions.SimpleCondition;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class Flink02 {
//
////    Logger.getLogger("org").setLevel(Level.ERROR);
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
//        streamEnv.setParallelism(1);
//            DataStream<String> dataStreamSource = streamEnv.socketTextStream("localhost", 9999);
//            SingleOutputStreamOperator<Pm> map = dataStreamSource.map(new RichMapFunction<String, Pm>() {
//                @Override
//                public Pm map(String s) throws Exception {
//                    String[] split = s.split(",");
//                    String host = split[0];
//                    String type = split[1];
//                    double action = Double.parseDouble(split[2]);
//
//                    return new Pm(host, type, action);
//                }
//            });
//
//        final NoSkipStrategy noSkipStrategy = AfterMatchSkipStrategy.noSkip();
//        final SkipPastLastStrategy skipPastLastStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
//        final SkipToLastStrategy skipToLastStrategy = AfterMatchSkipStrategy.skipToLast("stage1");
//        final AfterMatchSkipStrategy afterMatchSkipStrategy = AfterMatchSkipStrategy.skipToNext();
//
//        //设置忽略策略
//        final SkipToFirstStrategy skipToFirstStrategy = AfterMatchSkipStrategy.skipToFirst("stage1");
//
//        Pattern<Pm, Pm> pat = Pattern.<Pm>begin("stage1",skipToFirstStrategy)
//                .where(new SimpleCondition<Pm>() {
//                    @Override
//                    public boolean filter(Pm pm) throws Exception {
//                        return pm.getValue() > 80;
//                    }
//                 }).timesOrMore(5);
//
//        Pattern<Pm, Pm> pat2 = Pattern.<Pm>begin("stage1",skipToFirstStrategy).where(new SimpleCondition<Pm>() {
//            @Override
//            public boolean filter(Pm pm) throws Exception {
//                return pm.getValue() > 80;
//            }
//        }).followedBy("stage2").where(new SimpleCondition<Pm>() {
//            @Override
//            public boolean filter(Pm pm) throws Exception {
//
//                return pm.getValue() > 75;
//            }
//        }).timesOrMore(5);
//
//        HashMap<String, Tuple2<List<Pm>,List<Pm>>> buffPmbyHost = new HashMap<String, Tuple2<List<Pm>,List<Pm>>>();
//
//        Pattern.<Pm>begin("stage1").where(new SimpleCondition<Pm>() {
//            @Override
//            public boolean filter(Pm pm) throws Exception {
//                boolean flg1 = pm.getValue() > 80;
//                boolean flg = buffPmbyHost.containsKey(pm.getHost());
//                if (flg) {
//                    if (flg1){
//                        buffPmbyHost.get(pm.getHost()).f0.add(pm);
//                    }else{
//                        buffPmbyHost.get(pm.getHost()).f1.add(pm);
//                    }
//                } else {
//                    List pmSafeBuff = new ArrayList();
//                    List pmDangereBuff = new ArrayList();
//                    if (flg1){
//                        pmSafeBuff.add(pm);
//                    }else{
//                        pmDangereBuff.add(pm);
//                    }
//                    Tuple2<List<Pm>, List<Pm>> listListTuple2 = new Tuple2<List<Pm>, List<Pm>>(pmSafeBuff, pmDangereBuff);
//                    if(flg1){
//                        buffPmbyHost.put(pm.getHost(),listListTuple2);
//                    }
//                };
//                return flg1;
//            }
//        }).followedBy("stage2").where(new SimpleCondition<Pm>() {
//            @Override
//            public boolean filter(Pm pm) throws Exception {
//                boolean flg1 = buffPmbyHost.containsKey(pm.getHost());
//                boolean flg2 = pm.getValue() > 75;
////                if (flg1 && flg2) {
////                    buffPmbyHost.get(pm.getHost()).add(pm);
////                } else {
////                    List pmBuff = new ArrayList();
////                    pmBuff.add(pm);
////                    buffPmbyHost.put(pm.getHost(), pmBuff);
////                };
//                return flg1 && flg2;
//            }
//        });
//
//
//        PatternStream<Pm> pattern = CEP.pattern(map, pat2);
//        pattern.select(new RichPatternSelectFunction<Pm, String>() {
//            @Override
//            public String select(Map<String, List<Pm>> map) throws Exception {
//                List<Pm> stage1 = map.get("stage1");
//                List<Pm> stage2 = map.get("stage2");
//                Pm pm1 = stage1.get(stage1.size() - 1);
//                Pm pm2 = stage2.get(stage2.size() - 1);
//                return pm1.getHost()+","+pm1.getType()+","+pm1.getValue()+","+pm2.getValue()+","+stage1.size();
////                return pm1.getHost()+","+pm1.getType()+","+pm1.getValue()+","+stage1.size();
//            }
//        }).print();
//
//        streamEnv.execute("test");
//    }
//
//}
