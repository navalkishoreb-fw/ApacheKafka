package com.navalkishoreb.edu;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
public class KafkaConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

    @Bean
    public NewTopic topic() {
        return TopicBuilder.name("kafka-sample-topic-naval")
                .partitions(10)
                .replicas(1)
                .build();
    }

    @KafkaListener(id = "consumer-id-naval", topics = "kafka-sample-topic-naval")
    public void listen(String in) {
        System.out.println(in);
    }

}