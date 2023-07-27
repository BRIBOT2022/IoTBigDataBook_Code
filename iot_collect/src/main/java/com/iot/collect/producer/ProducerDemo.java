package com.iot.collect.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @description: TODO
 * @Author Juniors Lee
 * @Date 2022-09-28 15:05
 */
public class ProducerDemo {

    // 以下两个属性也可以作为send方法的参数，更灵活些
    private static Properties properties = getConf();

    public static void main(String[] args) {

        while (true){
            for (int i = 0; i < 100; i++) {

                send(String.valueOf(i));
            }
        }

    }

    static void send(String str) {
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<>(properties);
            ProducerRecord<String, String> record = new ProducerRecord<>("first", str);
            producer.send(record);
        } catch (Exception e) {
            System.out.println("dsdf");
        }
    }


    private static Properties getConf() {
        Properties properties = new Properties();
        // ProducerConfig 类定义了生产者相关的配置
        // 指定kafka 连接的集群
        properties.put("bootstrap.servers", "192.168.26.134:9092,192.168.26.135:9092,192.168.26.136:9092");
        // ack应答级别
        properties.put("acks", "all");
        // 重试次数
        properties.put("retries", 0);
        // 单次发送批次的大小
        properties.put("batch.size", 16384);
        // 批次大小不足的情况等待发送时间
        properties.put("linger.ms", 1);
        // RecordAccumulator 缓冲区大小
        properties.put("buffer.memory", 33554432);

        // key - value序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return properties;
    }

}
