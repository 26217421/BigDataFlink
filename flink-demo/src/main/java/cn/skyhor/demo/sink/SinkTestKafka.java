package cn.skyhor.demo.sink;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.sink
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/9 10:24
 */

import cn.skyhor.demo.day1.WordCount;
import cn.skyhor.demo.source.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.util.Objects;

/**
 * @ClassName: SinkTest1_Kafka
 * @Description:
 * @Author: wushengran on 2020/11/9 10:24
 * @Version: 1.0
 */
public class SinkTestKafka {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        String fileName = "sensor.txt";
        String path = Objects.requireNonNull(WordCount.class.getClassLoader().getResource(fileName)).getPath();
        DataStream<String> inputStream = env.readTextFile(path);

        // 转换成SensorReading类型
        DataStream<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });
        dataStream.print();
        dataStream.addSink(new FlinkKafkaProducer011<>("server103:9092,server103:9093,server103:9094", "sensor", new SimpleStringSchema()));

        env.execute();
    }
}
