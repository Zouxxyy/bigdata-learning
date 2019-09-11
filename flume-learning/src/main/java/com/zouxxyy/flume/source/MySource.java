package com.zouxxyy.flume.source;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;
    private String subfix;

    public Status process() throws EventDeliveryException {

        Status status = null;

        // 1 接受数据(这里是自定义数据)
        try {
            for (int i = 1; i < 6; i++) {
                SimpleEvent event = new SimpleEvent();

                // 2 封装成event
                event.setBody((prefix + "--" + i + "--" + subfix).getBytes());

                // 3 将事件传给channel
                getChannelProcessor().processEvent(event);

                // 4 将status设为READY
                status = Status.READY;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return status;
    }

    public long getBackOffSleepIncrement() {
        return 0;
    }

    public long getMaxBackOffSleepInterval() {
        return 0;
    }

    public void configure(Context context) {

        prefix = context.getString("prefix");
        subfix = context.getString("subfix", "zxy");

    }
}
