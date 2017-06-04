package rabbitmq.test.RPC;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * RPC远程服务服务端
 * Created by admin on 2017/6/3.
 */
public class RPCServer {
    //定义转发器的名字
    private final static String RPC_QUEUE_NAME = "rpc_queue";

    private static int fib(int n){
        if(n==0){return 0;}
        if(n==1){return 1;}
        return fib(n-1)+fib(n-2);
    }


    public static void main(String[] args) throws IOException, TimeoutException {
        //连接RabbitMQ服务
        ConnectionFactory factory = new ConnectionFactory();
        //服务器地址
        factory.setHost("localhost");
        //创建连接
        Connection connection = factory.newConnection();
        try{
            //创建频道
            final Channel channel = connection.createChannel();
            //定义转发器、类型
            channel.queueDeclare(RPC_QUEUE_NAME,false,false,false,null);
            //最大消息接收量
            channel.basicQos(1);
            System.out.println("Waiting RPC request.......");
            Consumer consumer = new DefaultConsumer(channel){
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    AMQP.BasicProperties replyProps = new AMQP.BasicProperties().builder().correlationId(properties.getCorrelationId()).build();
                    String responseMsg = "";
                    try {

                        String message = new String(body,"UTF-8");
                        int n = Integer.parseInt(message);
                        System.out.println("fib("+message+")");
                        responseMsg+=fib(n);
                    }finally {
                        //往队列发送消息
                        channel.basicPublish("",properties.getReplyTo(),replyProps,responseMsg.getBytes("UTF-8"));
                        //发送应答
                        channel.basicAck(envelope.getDeliveryTag(),false);
                    }
                }
            };

            channel.basicConsume(RPC_QUEUE_NAME,false,consumer);
            while(true){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }catch (Exception e){}finally {
            if(connection != null){
                //关闭连接
                connection.close();
            }
        }


    }
}
