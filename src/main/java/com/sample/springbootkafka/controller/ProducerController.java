package com.sample.springbootkafka.controller;

import com.sample.springbootkafka.model.MessageEntity;
//import com.sun.org.apache.xpath.internal.operations.String;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.*;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RestController
@RequestMapping("api")
public class ProducerController {

    @Value(value = "${spring.kafka.template.first-topic}")
    private String firstTopicName;

    @Value(value = "${spring.kafka.template.partition-topic}")
    private String secondTopicName;

    @Value(value = "${spring.kafka.template.transactional-topic}")
    private String thirdTopicName;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private Consumer<String, Object> manualConsumer;

    //POST localhost:8080/api/send
    @PostMapping("send")
    public ResponseEntity<?> sendMessage(){

        MessageEntity messageEntity = new MessageEntity("test", LocalDateTime.now());
        //kafkaTemplate.send(firstTopicName, "key", messageEntity);
        ListenableFuture<SendResult<String, Object>> future = kafkaTemplate.send(firstTopicName, messageEntity);
        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onFailure(Throwable ex) {

                System.out.println("Unable to send message: " + ex.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, Object> result) {

                System.out.println("Send message with offset: " + result.getRecordMetadata().offset());
            }
        });
        return ResponseEntity.ok(messageEntity);
    }
    //GET http://localhost:8080/api/manual
    @GetMapping("manual")
    public ResponseEntity<?> getMessagesManually(){

        TopicPartition partition = new TopicPartition(firstTopicName, 0);
        manualConsumer.assign(Arrays.asList(partition));
        manualConsumer.seek(partition, 0);
        ConsumerRecords<String, Object> records = manualConsumer.poll(Duration.ofMillis(1000));
        for(ConsumerRecord<String, Object> record : records){

            System.out.println(record);
        }
        manualConsumer.unsubscribe(); //Close Listener

        return ResponseEntity.ok(StreamSupport.stream(records.spliterator(), false)
                .map(r -> r.value())
                .collect(Collectors.toList()));
    }
    //POST api/partition/{key}
    @PostMapping("partition/{key}")
    public ResponseEntity<?> sendMessageToMultiplePartitions(@PathVariable String key){

        MessageEntity messageEntity = new MessageEntity(key, LocalDateTime.now());
        kafkaTemplate.send(secondTopicName, key, messageEntity);
        return ResponseEntity.ok(messageEntity);
    }
    //POST api/transactional/{key} ---> api/transactional/abc,key1
    @PostMapping("transactional/{key}")
    public ResponseEntity<?> sendTransactional(@PathVariable String key){

        List<MessageEntity> entityList = new ArrayList<>();
        kafkaTemplate.executeInTransaction(kt ->{
            String[] keyList = key.split(",");
            for (String str : keyList){

                if(str.length() > 2){

                    MessageEntity messageEntity =  new MessageEntity(str, LocalDateTime.now());
                    kt.send(thirdTopicName, str, messageEntity);
                    entityList.add(messageEntity);

                }
                else {

                    throw new RuntimeException();
                }
            }
            return null;
        });
        return ResponseEntity.ok(entityList);
    }

}
