import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
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
  private final static int port = 8000;

  public static void main(String[] argv) throws Exception {
    int numOfThreads = Integer.parseInt(argv[0]);

    ConnectionFactory factory = new ConnectionFactory();
    //factory.setHost("localhost");
    factory.setHost("ec2-3-86-116-243.compute-1.amazonaws.com");
    factory.setUsername("user1");
    factory.setPassword("pass1");
    final Connection connection = factory.newConnection();

    AmazonDynamoDB client = AmazonDynamoDBClientBuilder
        .standard()
        .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("dynamodb.us-east-1.amazonaws.com", "us-east-1"))
        .build();
    DynamoDB dynamoDB = new DynamoDB(client);
    Table table = dynamoDB.getTable("countTable");

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
            if (!pair[0].equals("")) {
              handler(table, pair[0], Long.parseLong(pair[1]));
            }

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

  private static void handler(Table table, String word, Long count) {
    UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("word", word)
        .withUpdateExpression("set counterme = if_not_exists(counterme, :start) + :val")
        .withValueMap(new ValueMap().withNumber(":val", count).withNumber(":start",0)).withReturnValues(ReturnValue.UPDATED_NEW);

    try {
      System.out.println("Incrementing an atomic counter...");
      UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
      System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());
    }
    catch (Exception e) {
      System.err.println("Unable to update item: " + word + " " + count);
      System.err.println(e.getMessage());
    }

  }

}

