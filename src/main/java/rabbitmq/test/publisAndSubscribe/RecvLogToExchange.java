package rabbitmq.test.publisAndSubscribe;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 发布/订阅模式---- 订阅者1，通过转发器接收消息在控制台输出
 * Created by admin on 2017/6/3.
 */
public class RecvLogToExchange {
    private final static String EXCHANGE_NAME = "logs";

    public static void main (String[] args) throws IOException, TimeoutException {
        //连接RabbitMQ服务器
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        //创建频道
        Channel channel = connection.createChannel();
        //定义转发器、类型
        channel.exchangeDeclare(EXCHANGE_NAME,"fanout");
        //创建一个非持久的、唯一的且自动删除的队列
        String queueName = channel.queueDeclare().getQueue();
        //为转发器指定队列
        channel.queueBind(queueName,EXCHANGE_NAME,"");
        System.out.println("Waiting for message,To exit prcess CTRL+C");
        //往队列发送消息
        channel.basicConsume(queueName,true, new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body,"UTF-8");
                System.out.println("Received '"+message+"'");
            }
        });
    }


}
