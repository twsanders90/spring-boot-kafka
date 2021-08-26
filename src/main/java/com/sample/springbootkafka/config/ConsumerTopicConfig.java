package com.sample.springbootkafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@EnableKafka
@Configuration
public class ConsumerTopicConfig {

    @Value(value = "localhost:9092")
    private String bootstrapAddress;

    public ConsumerFactory<String, Object> consumerFactory(String groupId){

        return consumerFactory(groupId, null);
    }

    public ConsumerFactory<String, Object> consumerFactory(String groupId, String isolationLevel){

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, Optional.ofNullable(isolationLevel).orElse(ConsumerConfig.DEFAULT_ISOLATION_LEVEL));

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("com.sample.springbootkafka.model");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), jsonDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> firstKafkaListenerContainerFactory(){

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("groupId1"));
        factory.setConcurrency(3);
        factory.setAutoStartup(true);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> filterKafkaListenerContainerFactory(){

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("groupFilter"));
        factory.setRecordFilterStrategy(consumerRecord -> "test".equals(consumerRecord.key()));
        factory.setConcurrency(3);
        factory.setAutoStartup(true);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> handlerKafkaListenerContainerFactory(){

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("groupHandler"));
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new FixedBackOff(500, 3)));
        factory.setConcurrency(3);
        factory.setAutoStartup(true);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> transactionalKafkaListenerContainerFactory(){

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("groupTransactional", "read_committed"));
        factory.setConcurrency(3);
        factory.setAutoStartup(true);
        return factory;
    }

    @Bean
    public Consumer<String, Object> manualConsumer(){

        return consumerFactory("groupManual").createConsumer();
    }
}
