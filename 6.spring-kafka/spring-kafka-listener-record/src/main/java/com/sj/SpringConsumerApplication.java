package com.sj;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;

@SpringBootApplication
public class SpringConsumerApplication {
    public static Logger logger = LoggerFactory.getLogger(SpringConsumerApplication.class);


    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(SpringConsumerApplication.class);
        application.run(args);
    }

    @KafkaListener(topics = "test",
            groupId = "con-group-record")
    public void recordListener(ConsumerRecord<String,String> record) {
        logger.info(record.toString());
    }

    @KafkaListener(topics = "test",
            groupId = "con-group-msg")
    public void singleTopicListener(String messageValue) {
        logger.info(messageValue);
    }

    @KafkaListener(topics = "test",
            groupId = "con-group-msg-opt", properties = {
            "max.poll.interval.ms:60000",
            "auto.offset.reset:earliest"
    })
    public void singleTopicWithPropertiesListener(String messageValue) {
        logger.info(messageValue);
    }

    @KafkaListener(topics = "test",
            groupId = "con-group-multi",
            concurrency = "3")
    public void concurrentTopicListener(String messageValue) {
        logger.info(messageValue);
    }

    @KafkaListener(topicPartitions =
            {
                    @TopicPartition(topic = "test1", partitions = {"0", "1"}),
                    @TopicPartition(topic = "test2", partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "3"))
            },
            groupId = "con-group-part-opt")
    public void listenSpecificPartition(ConsumerRecord<String, String> record) {
        logger.info(record.toString());
    }
}
