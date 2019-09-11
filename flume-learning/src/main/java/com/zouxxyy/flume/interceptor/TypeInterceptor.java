package com.zouxxyy.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeInterceptor implements Interceptor {

    private List<Event> addHeaderEvents;

    public void initialize() {

        addHeaderEvents = new ArrayList<Event>();
    }

    // 单个事件拦截
    public Event intercept(Event event) {

        // 1. 获取event的header
        Map<String, String> headers = event.getHeaders();

        // 2. 获取event的body
        String body = new String(event.getBody());

        // 3. 自定义头信息
        if(body.contains("hello")) {
            headers.put("type", "zouxxyy");
        }
        else {
            headers.put("type", "flume");
        }
        return event;
    }

    // 批量事件拦截
    public List<Event> intercept(List<Event> list) {

        addHeaderEvents.clear();

        for (Event event : list) {
            addHeaderEvents.add(intercept(event));
        }

        return addHeaderEvents;
    }

    public void close() {

    }

    // 记住这个不能忘记
    public static class Builder implements Interceptor.Builder {

        public Interceptor build() {
            return new TypeInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
