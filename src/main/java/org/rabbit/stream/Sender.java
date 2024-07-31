package org.rabbit.stream;

import com.rabbitmq.stream.*;

import java.io.IOException;
import java.util.UUID;

public class Sender {

    public static void main(String[] args) {
        Environment environment = Environment.builder().build();

        String stream = "hello-java-stream";
        environment.streamCreator().stream(stream).maxLengthBytes(ByteCapacity.GB(5)).create();
        Producer producer = environment.producerBuilder().stream(stream).build();
        String msg = "hello " + UUID.randomUUID().toString();
        producer.send(producer.messageBuilder().addData(msg.getBytes()).build(), (status) -> environment.close());
        System.out.println(" [x] '" + msg + "' message sent");
    }
}
