package rabbitmq.test.routing;

import com.rabbitmq.client.*;
import rabbitmq.test.publisAndSubscribe.ReceviesLog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * Created by admin on 2017/6/4.
 */
public class ReceiveLogsDirect {
    private final static String EXCHANGE_NAME = "direct_logs";
    private final static String EXCHANGE_TYPE = "direct";
    //日志类型绑定键
    private final static String[] LOGS_TYPE = {"info","warning","error"};

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

        //获取绑定键
        String severity = getSeverity();

        //为转发器指定绑定队列
        channel.queueBind(queueName,EXCHANGE_NAME,severity);
        System.out.println("Waiting for message,To exit prcess CTRL+C");
        //往队列发送消息
        channel.basicConsume(queueName,true, new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body,"UTF-8");
                String severity = getSeverity();
                //错误日志打印到硬盘文件
                if(severity.equals("error")){
                    toDisk(message);
                }
                System.out.println("Recevice: '"+message+"'");
            }
        });
    }


    /**
     * 随机产生一种日志类型
     */
    private static String getSeverity() {
        Random random = new Random();
        int ranVal = random.nextInt(3);
        return LOGS_TYPE[ranVal];
    }

    /**
     * 将日志信息写入磁盘
     * @param message
     */
    private static void toDisk(String message) {
        try {
            String dir = ReceviesLog.class.getClassLoader().getResource("").getPath();
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
