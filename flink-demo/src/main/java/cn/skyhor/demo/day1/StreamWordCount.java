package cn.skyhor.demo.day1;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author wbw
 * @date 2022-12-26 0:18
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        DataStream<String> dataStream = env.socketTextStream(host, port);
        DataStream<Tuple2<String, Integer>> wordCountDataset =
                dataStream.flatMap(new WordCount.MyflatMapper())
                        .keyBy(0)
                        .sum(1);
        wordCountDataset.print().setParallelism(1);
        env.execute();
    }
}
