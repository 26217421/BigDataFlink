package cn.skyhor.demo.transform;

import cn.skyhor.demo.source.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wbw
 * @date 2023-1-4 4:35
 */
public class Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStream<String> inputStream = env.readTextFile("D:\\dev\\work\\BigDataFlink\\flink-demo\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        } );
        dataStream.print("input");

        // 1. shuffle
        DataStream<String> shuffleStream = inputStream.shuffle();

        shuffleStream.print("shuffle");

        // 2. keyBy

        dataStream.keyBy("id").print("keyBy");

        // 3. global
        dataStream.global().print("global");

        env.execute();
    }

}
