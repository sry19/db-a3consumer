import com.google.common.util.concurrent.AtomicLongMap;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Consumer {
  private final static String QUEUE_NAME = "threadExQ";

  public static void main(String[] argv) throws Exception {
    int numOfThreads = Integer.parseInt(argv[0]);

    ConnectionFactory factory = new ConnectionFactory();
    //factory.setHost("localhost");
    factory.setHost("ec2-3-86-116-243.compute-1.amazonaws.com");
    factory.setUsername("user1");
    factory.setPassword("pass1");
    final Connection connection = factory.newConnection();

    AtomicLongMap<String> atomicLongMap = AtomicLongMap.create();

    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          final Channel channel = connection.createChannel();
          channel.queueDeclare(QUEUE_NAME, true, false, false, null);
          // max one message per receiver
          channel.basicQos(1);
          //System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");

          DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            String[] pair = message.split(" ");
            atomicLongMap.addAndGet(pair[0],Long.parseLong(pair[1]));
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            //System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
          };
          // process messages
          channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
        } catch (IOException ex) {
          Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
        }
      }
    };
    // start threads and block to receive messages
    for (int i=0; i<numOfThreads; i++) {
      Thread thread = new Thread(runnable);
      thread.start();
    }

  }

}

