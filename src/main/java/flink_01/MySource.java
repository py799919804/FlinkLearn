package flink_01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class MySource extends RichSourceFunction<String> {

    //初始化操作可以在open方法中执行
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
    //采集行为可以在run方法中执行
    @Override
    public void run(SourceContext<String> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
