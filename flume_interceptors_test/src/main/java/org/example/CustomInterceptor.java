package org.example;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

public class CustomInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    // 单个事件拦截
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        if (body[0] < 'z' && body[0] > 'a') {
            // 自定义头信息
            event.getHeaders().put("type", "letter");
        } else if (body[0] > '0' && body[0] < '9') {
            // 自定义头信息
            event.getHeaders().put("type", "number");
        }
        return event;
    }

    // 批量事件拦截
    @Override
    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new CustomInterceptor();
        }

        @Override
        public void configure(Context context) {
        }
    }
}