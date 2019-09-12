package com.zouxxyy.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySink extends AbstractSink implements Configurable {

    private String prefix;
    private String subfix;

    private Logger logger = LoggerFactory.getLogger(MySink.class);

    public Status process() throws EventDeliveryException {

        Status status = null;

        Channel channel = getChannel();

        // 获取transaction
        Transaction transaction = channel.getTransaction();

        // 开始
        transaction.begin();

        try{
            Event event = channel.take();

            if (event != null) {

                // 从event中提取前需要先判断是否为空
                String body = new String(event.getBody());

                logger.info(prefix + body + subfix);
            }

            // 提交
            transaction.commit();

            status = Status.READY;
        }
        catch (ChannelException e) {
            e.printStackTrace();

            // 回滚
            transaction.rollback();

            status = Status.BACKOFF;
        }
        finally {
            // 关闭
            transaction.close();
        }

        return status;
    }

    public void configure(Context context) {

        prefix = context.getString("prefix");
        subfix = context.getString("subfix", "zxy");
    }
}
