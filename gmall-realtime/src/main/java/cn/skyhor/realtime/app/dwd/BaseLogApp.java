package cn.skyhor.realtime.app.dwd;

import cn.skyhor.realtime.utils.MyKafkaUtil;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @author wbw
 */
public class BaseLogApp {

    public static final String ONE = "1";

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
                    public void open(Configuration parameters) {
                        firstVisitDateState = getRuntimeContext()
                                .getState(new ValueStateDescriptor<>("new-mid", String.class));
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    }
                    @Override
                    public JSONObject map(JSONObject value) throws Exception {
                        String isNew = value.getJSONObject("common").getString("is_new");
                        if (ONE.equals(isNew)) {
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
        //6.分流 ,使用 ProcessFunction将 ODS数据 拆分成启动、曝光以及页面数据以
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(
                new ProcessFunction<JSONObject, String>() {

                    @Override
                    public void processElement(JSONObject jsonObject, Context context,
                                               Collector<String> collector) throws Exception {
                        String startStr = jsonObject.getString("start");
                        if (startStr != null && startStr.length() > 0) { //将启动日志输出到侧流
                            context.output(new OutputTag<String>("start") { },
                                    jsonObject.toString());
                        } else { //为页面数据
                            collector.collect(jsonObject.toString());
                            JSONArray displays = jsonObject.getJSONArray("displays");
                            if (displays != null && displays.size() > 0) {
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject displayJson = displays.getJSONObject(i);
                                    displayJson.put("page_id", jsonObject.getJSONObject("page")
                                            .getString("pag_id"));
                                    context.output(new OutputTag<String>("display"){},
                                            displayJson.toString());
                                }  
                            }
                        }
                    }
                });
        DataStream<String> startDS = pageDS.getSideOutput(new OutputTag<String>("start") { });
        DataStream<String> displayDS = pageDS.getSideOutput(new OutputTag<String>("display") { });

        jsonObjDS.print();
        jsonWithNewFlagDS.print();
        pageDS.print("Page>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>>");

        pageDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_page_log"));
        startDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaProducer("dwd_display_log"));
        env.execute();

    }

}
