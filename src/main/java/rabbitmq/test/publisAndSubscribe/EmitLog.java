package rabbitmq.test.publisAndSubscribe;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * 发布/订阅模式---- 生产者，不直接给队列发送消息，只能给转发器发布消息，然后转发器将消息推送给队列。
 * Created by admin on 2017/6/3.
 */
public class EmitLog {
    //定义转发器的名字
    private final static String EXCHANGE_NAME = "logs";
    //转发器类型 Fanout:转发器将消息发送给他知道的所有的队列
    private final static String EXCHANGE_TYPE = "fanout";

    public static void main(String[] args) throws IOException, TimeoutException {
        //连接RabbitMQ服务
        ConnectionFactory factory = new ConnectionFactory();
        //服务器地址
        factory.setHost("localhost");
        //创建连接
        Connection connection = factory.newConnection();
        //创建频道
        Channel channel = connection.createChannel();
        //定义转发器、类型
        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
        String message = new Date().toLocaleString()+":log--->> hello";
        //往转发器上面发送消息，因此就不需要在第二个参数设置队列名称、第三个参数设置消息持久化了
        channel.basicPublish(EXCHANGE_NAME,"",null,message.getBytes("UTF-8"));
        System.out.println("send '"+message+"'");

        //关闭连接
        channel.close();
        connection.close();
    }
}
