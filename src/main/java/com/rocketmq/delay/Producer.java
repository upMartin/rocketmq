package com.rocketmq.delay;

import com.rocketmq.order.OrderStep;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setVipChannelEnabled(false);
        producer.setNamesrvAddr("192.168.161.129:9876;192.168.161.133:9876");
        producer.start();
        for (int i=0; i<10;i++){
            Message message = new Message("base", "Tag1",
                    ("hello world!"+i).getBytes());
            message.setDelayTimeLevel(2);
            SendResult result = producer.send(message);
            SendStatus status = result.getSendStatus();
            String msgId = result.getMsgId();
            int queueId = result.getMessageQueue().getQueueId();
            System.out.println("消息状态: "+result);
            TimeUnit.SECONDS.sleep(1);
        }
        producer.shutdown();
    }
}
