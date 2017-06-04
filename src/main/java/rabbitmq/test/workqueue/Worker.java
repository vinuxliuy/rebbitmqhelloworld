package rabbitmq.test.workqueue;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消费者-接收消息
 * Created by admin on 2017/6/3.
 */
public class Worker {
    private final static String TASK_QUEUE_NAME = "task_queue";

    public static void main (String[] args) throws IOException, TimeoutException {
        //连接RabbitMQ服务器
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        //创建频道
        final Channel channel = connection.createChannel();
        //声明队列，并持久化
        boolean durable = true;
        channel.queueDeclare(TASK_QUEUE_NAME,durable,false,false,null);
        System.out.println("Waiting for message,To exit prcess CTRL+C");
//        QueueingConsumer consumer = new QueueingConsumer(channel);
        //设置最大服务消息转发数量，告诉RabbitMQ不要在同一时间给一个工作者超过1条消息，换句话说只有我闲暇的时候你在给我发送下一条消息
        channel.basicQos(1);
        boolean autoAck = false; //打开消息应答机制,当某个工作者被杀死时，未完成的消息会被转移至其他工作者继续完成，以至于数据不会丢失；之后工作者应答RabbitMQ消息已被接收和处理，然后RabbitMQ可以自行将消息删除
        channel.basicConsume(TASK_QUEUE_NAME,autoAck, new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body,"UTF-8");
                System.out.println("Received '"+message+"'");
                try{
                    doWork(message);
                }finally {
                    System.out.println("Done");
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }
            }
        });
    }

    private static void  doWork(String msg) {
        for (char ch : msg.toCharArray()){
            if(ch == '.'){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
