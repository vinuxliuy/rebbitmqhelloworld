package rabbitmq.test.helloworld;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消费者-接收消息
 * Created by admin on 2017/6/3.
 */
public class Recv {
    private final static String QUEUE_NAME = "hello";

    public static void main (String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        //声明队列，主要是为了防止接收者先运行此程序，队列还不存在时创建队列
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        System.out.println("Waiting for message,To exit prcess CTRL+C");
        Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
               String message = new String(body,"UTF-8");
               System.out.println("Received '"+message+"'");
            }
        };
        channel.basicConsume(QUEUE_NAME,true,consumer);
    }
}
