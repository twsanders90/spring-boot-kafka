package com.sample.springbootkafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
@NoArgsConstructor // public MessageEntity();
@AllArgsConstructor //public MessageEntity(name, time);
@Data
public class MessageEntity {

    private String name;
    private LocalDateTime time;

}
