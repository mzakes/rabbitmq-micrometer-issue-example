package com.example.rabbitmq.micrometer;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableRabbit
public class RabbitMicrometerApplication {

    private static final int NUMBER_OF_MESSAGES = 5;

    private final CountDownLatch listenLatch = new CountDownLatch(NUMBER_OF_MESSAGES);

    static final String EXCHANGE_NAME = "spring-test-exchange";
    static final String ROUTING_KEY = "spring-test-routing-key";
    static final String QUEUE_NAME = "spring-test-queue";

    @Bean
    RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory){
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    Exchange configure(RabbitAdmin rabbitAdmin) {
        Exchange topicExchange = ExchangeBuilder.topicExchange(EXCHANGE_NAME).build();
        rabbitAdmin.declareExchange(topicExchange);
        Queue queue = QueueBuilder.durable(QUEUE_NAME).build();
        rabbitAdmin.declareQueue(queue);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(topicExchange).with(ROUTING_KEY).noargs());
        return topicExchange;
    }

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    MeterRegistry meterRegistry;


    public static void main(String[] args) throws InterruptedException {
        ConfigurableApplicationContext context = SpringApplication.run(RabbitMicrometerApplication.class, args);
        context.getBean(RabbitMicrometerApplication.class).runTest();
        context.close();
    }

    private void runTest() throws InterruptedException {
        for(int i = 0; i < NUMBER_OF_MESSAGES; i++) {
            this.rabbitTemplate.convertAndSend(EXCHANGE_NAME, ROUTING_KEY, "message_" + i);
        }
        if (this.listenLatch.await(10, TimeUnit.SECONDS)) {
            System.out.println("Expected " + NUMBER_OF_MESSAGES + " messages. Checking metric registry...");
            System.out.println("Counter returns: " + meterRegistry.find("rabbitmq.consumed").counter().count());
        }
    }

    @RabbitListener(queues = QUEUE_NAME)
    void handleMessage(String in) {
        System.out.println("Received: " + in);
        this.listenLatch.countDown();
    }
}
