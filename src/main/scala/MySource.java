import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class MySource extends RichSourceFunction<String> {

    private Random random = new Random();
    private boolean isRunning = true;
    @Override
    public void run(SourceContext sourceContext) throws Exception {

        List<String> city = new ArrayList<String>();
        city.add("北京");
        city.add("辽宁");
        city.add("吉林");
        city.add("四川");

        while(isRunning){
            String cityName = city.get(random.nextInt(city.size()));
            Long timestamp = new Date().getTime();
            Double temperature = random.nextDouble();
            String sensorReading = cityName+","+timestamp.toString()+","+temperature.toString();
            sourceContext.collect(sensorReading);
        }

    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
