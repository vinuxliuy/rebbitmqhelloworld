package rabbitmq.test.topics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * Topic ： 模糊匹配绑定键，将消息发送给相应的队列，例如：*.color.*会被发送至接收者C1；*.color会被发送给C2。
 * Created by admin on 2017/6/3.
 */
public class EmitLogTopic {
    //定义转发器的名字
    private final static String EXCHANGE_NAME = "topic_logs";
    //转发器类型 topic:将一个附带模糊匹配选择键的消息发送至与之匹配的绑定键的队列，如果找不到选择键与绑定键相匹配的队列，则消息不会发给任何队列。
    private final static String EXCHANGE_TYPE = "topic";

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

        //设置选择键
        String[] routing_keys = {"kernal.info", "cron.warning.logs","auth.info", "kernel.critical" };
        for(String routKey:routing_keys){
            String message = UUID.randomUUID().toString();
            //给转发器发送信息附带一个选择键
            channel.basicPublish(EXCHANGE_NAME,routKey,null,message.getBytes("UTF-8"));
            System.out.println("send routKey:'"+routKey+"' ,message:'"+message+"'");
        }

        //关闭连接
        channel.close();
        connection.close();
    }


}
