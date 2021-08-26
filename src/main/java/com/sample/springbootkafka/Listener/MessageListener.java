package com.sample.springbootkafka.Listener;

import com.sample.springbootkafka.model.MessageEntity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class MessageListener {
    //@KafkaListener(topics = "${spring.kafka.template.first-topic}", containerFactory = "firstKafkaListenerContainerFactory")
    public void listenFirstTopic(Object message){

        System.out.println("Received message in group groupId1:  " + message);
    }
    //@KafkaListener(topics = "${spring.kafka.template.first-topic}", containerFactory = "firstKafkaListenerContainerFactory")
    public void listenFirstTopicWithDetails(ConsumerRecord<String, MessageEntity> consumerRecord,
                                            @Payload MessageEntity messageEntity,
                                            @Header(KafkaHeaders.GROUP_ID)String groupId,
                                            @Header(KafkaHeaders.OFFSET) int offset,
                                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition){

        System.out.println("Received message with below details: " );
        System.out.println(consumerRecord);
        System.out.println(messageEntity);
        System.out.println(groupId);
        System.out.println(offset);
        System.out.println(partition);
    }

    @KafkaListener(containerFactory = "firstKafkaListenerContainerFactory", topicPartitions = {
            @TopicPartition(topic = "${spring.kafka.template.first-topic}", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))
    })
    //Fetch message from beginning for each time application is up.
    public void listenFirstTopicFromBeginning(ConsumerRecord<String, MessageEntity> consumerRecord){

        System.out.println(consumerRecord.value() + "with offset: " + consumerRecord.offset());
    }

    @KafkaListener(groupId = "groupPartition", topics = "${spring.kafka.template.partition-topic}")
    public void listenSecondTopic(Object message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition){

        System.out.println("--------------------");
        System.out.println("Received message with below details: ");
        System.out.println(message);
        System.out.println("partition: " + partition);
        System.out.println("--------------------");

    }
    //Filter key = "test"
    @KafkaListener(topics = "${spring.kafka.template.partition-topic}", containerFactory = "filterKafkaListenerContainerFactory")
    public void filterListeners(Object message){

        System.out.println("--------------------");
        System.out.println("Received message in filter with below details: ");
        System.out.println(message);
        System.out.println("--------------------");
    }

    @KafkaListener(topics = "${spring.kafka.template.partition-topic}", containerFactory = "handlerKafkaListenerContainerFactory")
    public void errorHandlerListener(Object message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY)String key){

        System.out.println("--------------------");
        System.out.println("Received message in error handler with below details: ");
        System.out.println(message);
        System.out.println("--------------------");
        if("error".equals(key)){

            throw new RuntimeException();
        }
    }

    @KafkaListener(topics = "${spring.kafka.template.transactional-topic}", containerFactory = "transactionalKafkaListenerContainerFactory")
    public void transactionalListener(Object message){

        System.out.println("--------------------");
        System.out.println("Received message in transactional with below details: ");
        System.out.println(message);
        System.out.println("--------------------");
    }
}
