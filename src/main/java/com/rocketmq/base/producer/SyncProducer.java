package com.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.concurrent.TimeUnit;

/**
 * 发送同步消息
 */
public class SyncProducer {
    /**
     * 1.创建消息生产者producer，并制定生产者组名
     * 2.指定Nameserver地址
     * 3.启动producer
     * 4.创建消息对象，指定主题Topic、Tag和消息体
     * 5.发送消息
     * 6.关闭生产者producer
     */
    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        producer.setVipChannelEnabled(false);
        producer.setNamesrvAddr("192.168.161.129:9876;192.168.161.133:9876");
        producer.start();
        for (int i=0; i<10;i++){
            Message message = new Message("base", "Tag1",
                    ("hello world!"+i).getBytes());
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
