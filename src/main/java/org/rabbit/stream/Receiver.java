package org.rabbit.stream;

import com.rabbitmq.stream.*;

public class Receiver {

    public static void main(String[] args) {
        Address entryPoint = new Address("rabbitmq", 5552);
        Environment environment = Environment.builder()
                .host(entryPoint.host())
                .port(entryPoint.port())
                .addressResolver(address -> entryPoint)
                .build();

        String stream = "hello-java-stream";
        environment.streamCreator().stream(stream).maxLengthBytes(ByteCapacity.GB(5)).create();

        Consumer consumer = environment.consumerBuilder()
                .stream(stream)
                .offset(OffsetSpecification.first())
                .messageHandler((unused, message) -> {
                    System.out.println("Received message: " + new String(message.getBodyAsBinary()));
                }).build();
    }
}
