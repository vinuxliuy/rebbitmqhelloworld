package rabbitmq.test.workqueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *  工作队列-消息创建者
 * Created by admin on 2017/6/3.
 */
public class NewTask {
    private final static String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] args) throws IOException, TimeoutException {
        //连接RabbitMQ服务
        ConnectionFactory factory = new ConnectionFactory();
        //服务器地址
        factory.setHost("localhost");
        //创建连接
        Connection connection = factory.newConnection();
        //创建频道
        Channel channel = connection.createChannel();
        //指定消息队列
        boolean durable = true; //设置队列持久化，防止异常情况队列丢失
        channel.queueDeclare(TASK_QUEUE_NAME,durable,false,false,null);

        //发送10条消息，一次在消息后面附加1-10个点
        for (int i = 0; i<10; i++){
            String dots = "";
            for (int j=0; j<= i; j++){
                dots+=".";
            }
            String message = "HelloWorld"+dots+dots.length();
            //往队列发送一条消息，并将信息设置为持久化（MessageProperties.PERSISTENT_TEXT_PLAIN），即使RabbitMQ异常退出也不会丢失信息
            channel.basicPublish("",TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN,message.getBytes("UTF-8"));
            System.out.println("send '"+message+"'");
        }


        //关闭连接
        channel.close();
        connection.close();
    }
}
