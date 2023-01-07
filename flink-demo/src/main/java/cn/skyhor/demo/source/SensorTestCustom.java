package cn.skyhor.demo.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.security.SecureRandom;
import java.util.HashMap;

/**
 * @author wbw
 * @date 2023-1-1 14:04
 */
public class SensorTestCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorReading> dataStream = env.addSource(new MySensor());
        dataStream.print();
        env.execute();
    }

    public static class MySensor implements SourceFunction<SensorReading> {
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            SecureRandom random = new SecureRandom();
            HashMap<String, Double> sensorMap = new HashMap<>();
            for (int i = 0; i < 5; i++) {
                sensorMap.put("sensor_" + (i+1), 60 + random.nextGaussian() * 20);
            }
            while (running) {
                for (String sessionId:sensorMap.keySet()
                     ) {
                    double newTemp = sensorMap.get(sessionId) + random.nextGaussian()*15;
                    sensorMap.put(sessionId, newTemp);
                    ctx.collect(new SensorReading(sessionId, System.currentTimeMillis(),
                            newTemp));
                    Thread.sleep(1000L);
                }
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }
    }
}
