package cn.skyhor.realtime.app.function;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @author wbw
 */
public interface DimAsyncJoinFunction<T> {

    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;

}
