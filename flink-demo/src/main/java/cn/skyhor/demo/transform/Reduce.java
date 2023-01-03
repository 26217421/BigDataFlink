package cn.skyhor.demo.transform;

import cn.skyhor.demo.source.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wbw
 * @date 2023-1-2 16:34
 */
public class Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\dev\\work\\BigDataFlink\\flink-demo\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        } );
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        DataStream<SensorReading> reduceStream = keyedStream.reduce((sensorReading, t1) -> new SensorReading(sensorReading.getId(), t1.getTimestamp(),
                Math.max(sensorReading.getTemperature(), t1.getTemperature())));
        reduceStream.print();
        env.execute();
    }
}
