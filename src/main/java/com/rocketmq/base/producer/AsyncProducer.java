package com.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

public class AsyncProducer {
    //发送异步消息
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setVipChannelEnabled(false);
        producer.setNamesrvAddr("192.168.161.129:9876;192.168.161.133:9876");
        producer.start();
        for (int i=0; i<10;i++){
            Message message = new Message("base", "Tag2",
                    ("hello world!"+i).getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送结果: "+sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println("发送异常: "+throwable.getMessage());
                }
            });

            TimeUnit.SECONDS.sleep(1);
        }
        producer.shutdown();
    }
}
