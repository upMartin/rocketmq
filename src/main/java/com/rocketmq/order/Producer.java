package com.rocketmq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.List;

public class Producer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        /*producer.setVipChannelEnabled(false);*/
        producer.setNamesrvAddr("192.168.161.129:9876;192.168.161.133:9876");
        producer.start();
        List<OrderStep> list = OrderStep.buildOrders();
        //发送消息
        for (int i=0;i<list.size();i++){
            String body = list.get(i)+"";
            Message message = new Message("OrderTopic", "Order", i+"", body.getBytes());
            /**
             * 参数一: 消息对象
             * 参数二: 消息队列的选择器
             * 参数三: 选择队列的业务标识
             */
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Long id = (Long) o;  //根据订单id选择发送queue
                    long index = id % list.size();
                    return list.get((int) index);
                }
            }, list.get(i).getOrderId());
            System.out.println("发送结果: "+sendResult);
        }
        producer.shutdown();
    }
}
