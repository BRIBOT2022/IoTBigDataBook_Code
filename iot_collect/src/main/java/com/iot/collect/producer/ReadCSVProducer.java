package com.iot.collect.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @description: TODO
 * @Author Juniors Lee
 * @Date 2022-09-28 10:26
 */
public class ReadCSVProducer {

    public static void main(String[] args) throws FileNotFoundException {

        Properties props = new Properties();

        props.put("bootstrap.servers", "192.168.26.134:9092");//kafka本机地址及端口号
        props.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //kafka 生产者
        KafkaProducer<String, String> myProducer = new KafkaProducer<>(props);

        //文件
        FileInputStream csvFile = null;
        InputStreamReader is = null;
        BufferedReader br = null;

        //改成相对路径
        csvFile = new FileInputStream("src/main/java/com/iot/collect/data/sea.csv");

        while (true){
            try {
                is = new InputStreamReader(csvFile);
                br = new BufferedReader(is);
                String line = br.readLine();
                Long position = 0L;
                int i = 0;
                while (line != null) {
                    position += line.length();
                    System.out.println(line);
                    myProducer.send(new ProducerRecord<String, String>("first",line));
                    line = br.readLine();
                    i++;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
