package org.rabbit.woker;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;
import java.util.UUID;

public class Producer {

    private static final Logger LOG = Logger.getLogger(Producer.class.getName());
    ConnectionFactory factory;
    String queueName;
    String returnQueue;

    public Producer(String host, String queueName) {
        this(host, queueName, null);
    }

    public Producer(String host, String queueName, String returnQueue) {
        factory = new ConnectionFactory();
        factory.setHost(host);
        this.queueName = queueName;
        this.returnQueue = returnQueue;
    }

    public void send(String message) throws Exception {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            AMQP.BasicProperties props = null;
            if (returnQueue != null) {
                props = new AMQP.BasicProperties
                        .Builder()
                        .replyTo(returnQueue)
                        .build();
            }

            publish(message, channel, props);
        }
    }

    public String syncSend(String message) throws Exception {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            final String corrId = UUID.randomUUID().toString();

            String replyQueueName = channel.queueDeclare().getQueue();
            HashMap<String,Object> headers = new HashMap<>();
            headers.put("sync",true);
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(corrId)
                    .headers(headers)
                    .replyTo(replyQueueName)
                    .build();

            publish(message, channel, props);

            final CompletableFuture<String> response = new CompletableFuture<>();
            final CompletableFuture<Boolean> isError = new CompletableFuture<>();

            String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
                if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                    response.complete(new String(delivery.getBody(), "UTF-8"));
                    isError.complete(isError(delivery));
                }
            }, consumerTag -> {
            });

            String result = response.get();
            channel.basicCancel(ctag);
            if(isError.get())
                throw new RuntimeException(result);
            return result;
        }
    }

    private static boolean isError(Delivery delivery) {
        return delivery.getProperties().getHeaders() != null && (boolean) delivery.getProperties().getHeaders().get("error");
    }

    private void publish(String message, Channel channel, AMQP.BasicProperties props) throws IOException {
        // durable, non-exclusive, non-autodelete, no specific conf
        channel.queueDeclare(queueName, true, false, false, null);
        channel.basicPublish("", queueName, props, message.getBytes(StandardCharsets.UTF_8));
        LOG.info(queueName + " sent: " + message);
    }
}
