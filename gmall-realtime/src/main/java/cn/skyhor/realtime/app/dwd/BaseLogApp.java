package cn.skyhor.realtime.app.dwd;

import cn.skyhor.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * @author wbw
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        String topic = "ods_base_log";
        String groupId = "ods_dwd_base_log_app";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSONObject::parseObject);
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"));
        //5.使用状态做新老户校验 使用状态做新老户校验 使用状态做新老户校验 使用状态做新老户校验 使用状态做新老户校验 使用状态做新老户校验
        SingleOutputStreamOperator<JSONObject> jsonWithNewFlagDS = keyedStream
                .map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> firstVisitDateState;
                    private SimpleDateFormat simpleDateFormat;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDateState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<String>("new-mid", String.class));
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd"); }
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");
                        if ("1".equals(isNew)) {
                            //取出状态数据并当前访问时间
                            String firstDate = firstVisitDateState.value();
                            Long ts = value.getLong("ts");
                            //判断状态数据是否为Null
                            if (firstDate != null) { //修复
                                value.getJSONObject("common").put("is_new", "0");
                            }
                            else {
                                //更新状态
                                firstVisitDateState.update(simpleDateFormat.format(ts));
                            }
                        } //返回数据 返回数据
                        return value;
                    }});



        jsonObjDS.print();
        jsonWithNewFlagDS.print();
        env.execute();
    }
}
