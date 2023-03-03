package cn.skyhor.realtime.app.dwd;

import cn.skyhor.realtime.app.function.TableProcessFunction;
import cn.skyhor.realtime.bean.TableProcess;
import cn.skyhor.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import javax.annotation.Nullable;

/**
 * @author wbw
 */
public class BaseDbApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        String topic = "ods_base_db_";
        String groupId = "ods_db_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                (FilterFunction<JSONObject>) jsonObject -> {
                    String data = jsonObject.getString("data");
                    return data != null && data.length() > 0;
                }
        );

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("gmall2021-realtime")
                .tableList("gmall2021-realtime.table_process")
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }

                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {
                        String topic = sourceRecord.topic();
                        String[] split = topic.split("\\.");
                        String db = split[1];
                        String table = split[2];
                        Struct value = (Struct) sourceRecord.value();
                        Struct after = value.getStruct("after");
                        JSONObject data = new JSONObject();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(), after.get(field.name()));
                            }
                        }
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        JSONObject result = new JSONObject();
                        result.put("database", db);
                        result.put("table", table);
                        result.put("type", operation.toString().toLowerCase());
                        result.put("data", data);
                        collector.collect(result.toJSONString());
                    }
                }).build();

        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
        MapStateDescriptor<String, TableProcess> mapStateDescriptor =
                new MapStateDescriptor<>("table-process-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(mapStateDescriptor);
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);

        OutputTag<JSONObject> hbaseTag = new
                OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
                };
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = connectedStream.process(new
                TableProcessFunction(hbaseTag, mapStateDescriptor));
        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);

        kafkaJsonDS.addSink(MyKafkaUtil.getKafkaProducer(
                (KafkaSerializationSchema<JSONObject>) (element, timestamp) ->
                        new ProducerRecord<>(element.getString("sinkTable"),
                                element.getString("after").getBytes())));
        filterDS.print();

        env.execute();
    }

}
