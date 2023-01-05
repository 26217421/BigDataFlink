package cn.skyhor.demo.processfunction;

import cn.skyhor.demo.source.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author wbw
 * @date 2023-1-2 17:37
 */
public class SideOutCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\dev\\work\\BigDataFlink\\flink-demo\\src\\main\\resources\\sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        } );
        OutputTag<SensorReading> ATAG = new OutputTag<>("High", TypeInformation.of(SensorReading.class));
        OutputTag<SensorReading> BTAG = new OutputTag<>("Low", TypeInformation.of(SensorReading.class));

        SingleOutputStreamOperator<SensorReading> mainStream = dataStream.process(
                new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading value, ProcessFunction<SensorReading, SensorReading>.Context ctx, Collector<SensorReading> out) throws Exception {
                        out.collect(value);
                        if(value.getTemperature() > 30) {
                            ctx.output(ATAG, value);
                        } else {
                            ctx.output(BTAG, value);
                        }
                    }
                }
        );
        DataStream<SensorReading> highTempStream = mainStream.getSideOutput(ATAG);
        DataStream<SensorReading> lowTempStream = mainStream.getSideOutput(BTAG);

        highTempStream.print("high");
        lowTempStream.print("low");
        mainStream.print("all");

        // 2. 合流 connect，将高温流转换成二元组类型，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highTempStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowTempStream);

        DataStream<Object> resultStream = connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return new Tuple3<>(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), "normal");
            }
        });

        resultStream.print();

        // 3. union联合多条流
//        warningStream.union(lowTempStream);
        highTempStream.union(lowTempStream, mainStream);

        env.execute();
    }
}
