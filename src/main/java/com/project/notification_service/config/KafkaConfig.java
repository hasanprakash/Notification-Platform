package com.project.notification_service.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
// import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import com.project.notification_service.entity.Notification;

@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // Consumer factory
    @Bean
    public ConsumerFactory<String, Notification> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Listener container factory (Spring looks for this bean by default)
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Notification> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Notification> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    // Producer factory
    @Bean
    public ProducerFactory<String, Notification> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    // KafkaTemplate for sending messages
    @Bean
    public KafkaTemplate<String, Notification> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
