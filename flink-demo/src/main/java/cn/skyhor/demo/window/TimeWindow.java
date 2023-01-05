package cn.skyhor.demo.window;

import cn.skyhor.demo.source.SensorReading;
import cn.skyhor.demo.source.SensorTestCustom;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author wbw
 * @date 2023-1-4 19:05
 */
public class TimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SensorReading> dataStream = env.addSource(new SensorTestCustom.MySensor());

        // 1. 增量聚合函数
        DataStream<Integer> resultStream = dataStream.keyBy("id")
//                .countWindow(10, 2);
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        // 2. 全窗口函数
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//                })
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, org.apache.flink.streaming.api.windowing.windows.TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, org.apache.flink.streaming.api.windowing.windows.TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });

        // 3. 其它可选API
        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
//                .trigger()
//                .evictor()
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .sum("temperature");

        resultStream.print("result");

        sumStream.getSideOutput(outputTag).print("late");

        resultStream2.print();


        env.execute();
    }
}
