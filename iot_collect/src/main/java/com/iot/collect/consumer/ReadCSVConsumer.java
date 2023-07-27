package com.iot.collect.consumer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @description: TODO
 * @Author Juniors Lee
 * @Date 2022-09-28 10:22
 */
public class ReadCSVConsumer {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.26.134:9092");
        properties.setProperty("group.id", "csvtoflink");

        DataStream<String> myStream = env
                .addSource(new FlinkKafkaConsumer<>("first", new SimpleStringSchema(), properties));

        myStream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;
            @Override
            public String map(String value) throws Exception {
                return "Stream Value: " + value;
            }
        }).print();

        env.execute();
    }
}
