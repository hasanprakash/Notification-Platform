package com.project.notification_service.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfigurator {
    
    // Supposed to create the topic at start up
    @Bean
    public NewTopic notificationTopic() {
        System.out.println("Creating a topic: notification-events");
        return TopicBuilder.name("notification-events")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
