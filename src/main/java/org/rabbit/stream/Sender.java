package org.rabbit.stream;

import com.rabbitmq.stream.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

public class Sender {

    public static void main(String[] args) throws InterruptedException {
        Address entryPoint = new Address("rabbitmq", 5552);
        Environment environment = Environment.builder()
                .host(entryPoint.host())
                .port(entryPoint.port())
                .addressResolver(address -> entryPoint)
                .build();
        int messageCount = 20;

        String stream = "hello-java-stream";
        environment.streamCreator().stream(stream).maxLengthBytes(ByteCapacity.GB(5)).create();
        Producer producer = environment.producerBuilder().stream(stream).build();
        String msg = "hello " + UUID.randomUUID().toString();
        //producer.send(producer.messageBuilder().addData(msg.getBytes()).build(), (status) -> environment.close());
/*
        CountDownLatch publishConfirmLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            msg = i + " hello test " + UUID.randomUUID().toString();
            producer.send(producer.messageBuilder().addData(msg.getBytes()).build(), confirmationStatus -> publishConfirmLatch.countDown());
        }

        publishConfirmLatch.await();
        environment.close();

 */

        while(true){
            msg = "hello " + UUID.randomUUID().toString();
            producer.send(producer.messageBuilder().addData(msg.getBytes()).build(), null);
            System.out.println(" [x] '" + msg + "' message sent");
            Thread.sleep(10000);
        }
    }
}
