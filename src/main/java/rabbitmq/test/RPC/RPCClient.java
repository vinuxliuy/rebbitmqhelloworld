package rabbitmq.test.RPC;

import com.rabbitmq.client.*;
import rabbitmq.test.publisAndSubscribe.ReceviesLog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;

/**
 *  RPC客户端
 * Created by admin on 2017/6/4.
 */
public class RPCClient {
    private final static String RPC_QUEUE_NAME = "rpc_queue";
    private Connection connection;
    private Channel channel;
    private String replyQueueName ;

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
        this.replyQueueName = channel.queueDeclare().getQueue();
    }

    private String call(String message) throws IOException, InterruptedException{
        final String corrId = UUID.randomUUID().toString();
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(corrId).replyTo(replyQueueName).build();
        channel.basicPublish("",RPC_QUEUE_NAME,props,message.getBytes("UTF-8"));
        final BlockingQueue<String> response = new ArrayBlockingQueue<String>(1);
        channel.basicConsume(replyQueueName,true,new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                if(properties.getCorrelationId().equals(corrId)){
                    response.offer(new String(body,"UTF-8"));
                }
            }
        });
        return response.take();
    }

    private void close(){
        try {
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main (String[] args) {
        RPCClient rpcClient = null;
        try {
            String response ="";
            rpcClient = new RPCClient();
            System.out.println("Requesting fib(30)");
            response = rpcClient.call("30");
            System.out.println("Get '"+response+"'");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (rpcClient != null){
                rpcClient.close();
            }
        }

    }
}
