package rabbitmq.test.helloworld;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 生产者-用来连接RabbitMQ,并向其发送一个消息
 * Created by admin on 2017/6/3.
 */
public class Send {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, TimeoutException {
        //连接RabbitMQ服务
        ConnectionFactory factory = new ConnectionFactory();
        //服务器地址
        factory.setHost("localhost");
        //创建连接
        Connection connection = factory.newConnection();
        //创建频道
        Channel channel = connection.createChannel();
        //指定消息队列,队列只会在不存在的时候才会创建，多次声明并不会重复创建
        channel.queueDeclare(QUEUE_NAME,false,false,false,null);
        String message = "helloworld";
        //往队列发送一条消息，消息的内容是字节数组，意味着可以传递任何类型的元素
        channel.basicPublish("",QUEUE_NAME,null,message.getBytes());
        System.out.println("send '"+message+"'");
        //关闭连接
        channel.close();
        connection.close();
    }
}
