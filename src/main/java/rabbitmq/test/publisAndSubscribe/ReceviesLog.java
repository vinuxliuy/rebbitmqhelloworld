package rabbitmq.test.publisAndSubscribe;

import com.rabbitmq.client.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * 发布/订阅模式---- 订阅者2，通过转发器接收消息写入磁盘
 * Created by admin on 2017/6/3.
 */
public class ReceviesLog {
    private final static String EXCHANGE_NAME = "logs";
    private final static String EXCHANGE_TYPE = "fanout";

    public static void main (String[] args) throws IOException, TimeoutException {
        //连接RabbitMQ服务器
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        //创建频道
        Channel channel = connection.createChannel();
        //定义转发器、类型
        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
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
//                System.out.println("Received '"+message+"'");
                toDisk(message);
            }
        });
    }

    /**
     * 将日志信息写入磁盘
     * @param message
     */
    private static void toDisk(String message) {
        try {
            String dir = ReceviesLog.class.getClassLoader().getResource("").getPath();
            System.out.println("dir============="+dir);
            String fileName = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            File file = new File(dir,fileName+".txt");
            FileOutputStream outputStream = new FileOutputStream(file);
            try {
                outputStream.write(message.getBytes("UTF-8"));
                outputStream.flush();
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }catch (FileNotFoundException e){
            e.printStackTrace();
        }
    }
}
