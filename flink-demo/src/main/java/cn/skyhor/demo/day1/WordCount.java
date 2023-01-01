package cn.skyhor.demo.day1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * @author wbw
 * @date 2022-12-25 12:09
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String fileName = "hello.txt";
        String path = Objects.requireNonNull(WordCount.class.getClassLoader().getResource(fileName)).getPath();
        System.out.println(path);
        DataSet<String> dataSet = env.readTextFile(path);
        DataSet<Tuple2<String, Integer>> wordCountDataset =
                dataSet.flatMap(new MyflatMapper())
                        .groupBy(0)
                        .sum(1);
        wordCountDataset.print();
    }

    public static class MyflatMapper implements FlatMapFunction<String,
            Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String,
                Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String i:words
                 ) {
                out.collect(new Tuple2<String, Integer>(i, 1));
            }
        }
    }
}
