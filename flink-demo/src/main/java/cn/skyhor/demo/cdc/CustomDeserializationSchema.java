package cn.skyhor.demo.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author wbw
 */
public class CustomDeserializationSchema implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) {
        String topic = sourceRecord.topic();
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        Struct value = (Struct) sourceRecord.value();
        Struct after = value.getStruct("after");
        JSONObject data = new JSONObject();
        for (Field field : after.schema().fields()) {
            Object o = after.get(field);
            data.put(field.name(), o);
        }
        JSONObject result = new JSONObject();
        result.put("operation", operation.toString().toLowerCase());
        result.put("data", data);
        result.put("database", db);
        result.put("table", tableName);
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
