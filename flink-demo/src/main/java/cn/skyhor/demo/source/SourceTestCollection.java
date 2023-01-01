package cn.skyhor.demo.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author wbw
 * @date 2022-12-30 14:05
 */
public class SourceTestCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorReading> sensorReadingDataStream = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_2", 1547718201L, 15.4),
                        new SensorReading("sensor_3", 1547718202L, 6.7),
                        new SensorReading("sensor_4", 1547718205L, 38.1)
                )
        );
        sensorReadingDataStream.print();
        env.execute();
    }


}
