package com.kafka.test;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * created with idea
 * user:ztwu
 * date:2019/5/28
 * description
 */
public class ConsumerDemo {

    public static KafkaConsumer<String, String> consumer;

    public ConsumerDemo() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.100:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String,String>(props);
    }

    public ConsumerDemo(String isAutoCommit) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.100:9092");
        props.put("group.id", "test2");
        //设置不自动提交，自己手动更新offset
        props.put("enable.auto.commit", isAutoCommit);
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String,String>(props);
    }

    public void getTopicData(){
        consumer.subscribe(Arrays.asList("ztwu"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }

    public void getTopicData2(){
        consumer.subscribe(Arrays.asList("ztwu"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                //消费者手动提交offset
                consumer.commitAsync();
            }
        }
    }

    public void getTopicData3(){

        consumer.subscribe(Collections.singletonList("ztwu"), new ConsumerRebalanceListener() {

            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }

            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                Map<TopicPartition,Long> beginningOffset = consumer.beginningOffsets(collection);

                //读取历史数据 --from-beginning
                for(Map.Entry<TopicPartition,Long> entry : beginningOffset.entrySet()){
                    // 基于seek方法
                    //TopicPartition tp = entry.getKey();
                    //long offset = entry.getValue();
                    //consumer.seek(tp,offset);

                    // 基于seekToBeginning方法
                    consumer.seekToBeginning(collection);
                }
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                //消费者手动提交offset
                consumer.commitAsync();
            }
        }
    }

    public void getTopics(){
        Map<String, List<PartitionInfo>> map = consumer.listTopics();
        if (map != null && !map.isEmpty()) {
            for(Map.Entry entry:map.entrySet()){
                System.out.println(entry.getKey());
            }
        }
    }

    public void getTopicPartitions(){
        String topic = "ztwu100";
        List<PartitionInfo> list = consumer.partitionsFor(topic);
        for(PartitionInfo partitionInfo : list){
            System.out.println(partitionInfo.partition()+" = "+partitionInfo.topic());
        }
    }

    public static void main(String[] args) {
        ConsumerDemo consumerDemo = new ConsumerDemo();
//        consumerDemo.getTopicData();
//        consumerDemo.getTopics();
//        consumerDemo.getTopicPartitions();

        ConsumerDemo consumerDemo1 = new ConsumerDemo("false");
        consumerDemo1.getTopicData2();
//        consumerDemo1.getTopicData3();

        consumer.close();

    }

}
