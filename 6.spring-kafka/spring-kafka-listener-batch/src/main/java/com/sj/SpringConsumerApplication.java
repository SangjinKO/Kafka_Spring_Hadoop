package com.sj;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

import java.util.List;

@SpringBootApplication
public class SpringConsumerApplication {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);

    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }

    @KafkaListener(topics = "test",
            groupId = "con-group-record")
    public void batchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> logger.info(record.toString()));
    }

    @KafkaListener(topics = "test",
            groupId = "con-group-list")
    public void batchListener(List<String> list) {
        list.forEach(recordValue -> logger.info(recordValue));
    }

    @KafkaListener(topics = "test",
            groupId = "con-group-multi",
            concurrency = "3")
    public void concurrentBatchListener(ConsumerRecords<String, String> records) {
        records.forEach(record -> logger.info(record.toString()));
    }

}
