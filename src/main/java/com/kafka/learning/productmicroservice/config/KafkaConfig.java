package com.kafka.learning.productmicroservice.config;

import com.kafka.learning.productmicroservice.service.event.ProductCreatedEvent;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

//
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.producer.key-serializer}")
    private String keySerializer;
    @Value("${spring.kafka.producer.value-serializer}")
    private String valueSerializer;
    @Value("${spring.kafka.producer.acks}")
    private String acks;
    @Value("${spring.kafka.producer.properties.delivery.timeout.ms}")
    private String deliveryTimeout;
    @Value("${spring.kafka.producer.properties.linger.ms}")
    private String linger;
    @Value("${spring.kafka.producer.properties.request.timeout.ms}")
    private String requestTimeout;
    @Value("${spring.kafka.producer.properties.enable.idempotence}")
    private String idempotence;

    Map<String, Object> producerConfigs() {
        Map<String, Object> config = new HashMap<>();

        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        config.put(ProducerConfig.ACKS_CONFIG, acks);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout);
        config.put(ProducerConfig.LINGER_MS_CONFIG, linger);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
        return config;
    }

    // фабрика которая выдает конфиги
    @Bean
    ProducerFactory<String, ProductCreatedEvent> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    // создаем бин который будет возвращать KafkaTemplate на основании
    // нашей кастомной фабрики
    // Теперь вместо дефолтного KafkaTemplate будм использовать свой собственный
    @Bean
    KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    NewTopic createTopic(){
        return TopicBuilder.name("product-created-events-topic")
                .partitions(3)
                .replicas(3)
                .configs(Map.of("min.insync.replicas", "2")) // 2 сервера должны быть в синхроне с нашим лидером
                .build();
    }
    // Этот бин я создал т.к ошибка непонятная хоть я и прописал в конфиге порты брокеров
    // он без этого метода пытается слушать брокеров по другим портам

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9192");
        return new KafkaAdmin(configs);
    }


}
