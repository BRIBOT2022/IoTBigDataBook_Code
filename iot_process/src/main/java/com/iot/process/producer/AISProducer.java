package com.iot.process.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * 将source.csv中的数据发送到Kafka中
 * @author IoTBigdataBook
 * @create 2023-01-15 20:06
 */
public class AISProducer {
    public static void main(String[] args) throws IOException {
        //创建Properties集合，用于保存kafka配置信息
        Properties props = new Properties();
        //kafka本机地址及端口号
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        //所有follower都响应了才认为消息提交成功，即"committed"
        props.put("acks", "all");
        //指定对应的key和value的序列化类型
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建kafka生产者对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        //获取输入字符流
        BufferedReader br = new BufferedReader(new FileReader("data/source.csv"));

        //从文件中按行读取数据
        String line;
        while ((line = br.readLine()) != null) {
            //将数据传入kafka的first主题中
            kafkaProducer.send(new ProducerRecord<String, String>("first", line));
        }

        //关闭资源
        kafkaProducer.close();
        br.close();
    }
}
