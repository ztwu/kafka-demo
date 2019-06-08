package com.kafka.test;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * created with idea
 * user:ztwu
 * date:2019/5/28
 * description
 */
public class ProducorDemo {

    public static Producer<String, String> producer;

    public ProducorDemo() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.100:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public ProducorDemo(String partitioner) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.100:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        //这个地方指定了分区器，如果不指定就是默认的
        props.put("partitioner.class", partitioner);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public void pubData1(){
        // send 方法是异步的，方法返回并不代表消息发送成功
        producer.send(new ProducerRecord<String, String>("ztwu", "message 1"));
        producer.close();
    }

    public void pubData2(){
        // send 方法是异步的，方法返回并不代表消息发送成功
        // 如果需要确认消息是否发送成功，以及发送后做一些额外操作，有两种办法
        // 方法 1: 使用 callback
        for(int i=0; i<5; i++){
            producer.send(new ProducerRecord<String, String>("ztwu", "message 2 = "+i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception != null) {
                        System.out.println("send message2 failed with " + exception.getMessage());
                    } else {
                        // offset 是消息在 partition 中的编号，可以根据 offset 检索消息
                        System.out.println("message2 sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }
        producer.close();
    }

    public void pubData3(){
        // send 方法是异步的，方法返回并不代表消息发送成功
        // 如果需要确认消息是否发送成功，以及发送后做一些额外操作，有两种办法
        // 方法2：使用阻塞
        for(int i=0; i<5; i++){
            Future<RecordMetadata> sendResult = producer.send(new ProducerRecord<String, String>("ztwu", "message 3 = "+i));
            try {
                // 阻塞直到发送成功
                RecordMetadata metadata = sendResult.get();
                System.out.println("message3 sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } catch(Exception e) {
                System.out.println("send message3 failed with " + e.getMessage());
            }
        }
        producer.close();
    }

    public void pubData4(){
        for (int i = 1; i <= 1000; i++) {
            String value = "value_" + i;
            //构建生产记录， 这里指定了key为123
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>("ztwu300", "123", value);
            RecordMetadata metadata = null;
            try {
                metadata = producer.send(msg).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            //打印分区信息
            String result = "value [" + msg.value() + "] has been sent to partition " + metadata.partition();
            System.out.println(result);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("send message over.");
        producer.close(100, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args){
        ProducorDemo producorDemo = new ProducorDemo();
//        producorDemo.pubData1();
//        producorDemo.pubData2();
//        producorDemo.pubData3();

        ProducorDemo producorDemo1 = new ProducorDemo("com.kafka.test.CustomerParatitioner");
        producorDemo1.pubData4();

    }

}
