package rabbitmq.test.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * 通过转发器绑定的key向队列绑定的与之匹配的发送消息
 * Created by admin on 2017/6/3.
 */
public class EmitLogDirect {
    //定义转发器的名字
    private final static String EXCHANGE_NAME = "direct_logs";
    //转发器类型 direct:将一个附带选择键的消息发送至与之匹配的绑定键的队列，如果找不到选择键与绑定键相匹配的队列，则消息不会发给任何队列。
    private final static String EXCHANGE_TYPE = "direct";
    //日志类型绑定键
    private final static String[] LOGS_TYPE = {"info","warning","error"};

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
        for (int i=0;i<6;i++){
            String severity = getSeverity();
            String message = severity+":log--->>"+(new Date().toLocaleString());
            //给转发器发送信息附带一个绑定键
            channel.basicPublish(EXCHANGE_NAME,severity,null,message.getBytes("UTF-8"));
            System.out.println("send '"+message+"'");
        }
        //关闭连接
        channel.close();
        connection.close();
    }

    //随机产生一种日志类型
    private static String getSeverity() {
        Random random = new Random();
        int ranVal = random.nextInt(3);
        return LOGS_TYPE[ranVal];
    }
}
