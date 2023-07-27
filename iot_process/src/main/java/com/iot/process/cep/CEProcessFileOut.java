package com.iot.process.cep;

import com.iot.process.pojo.AISEvent;
import com.iot.process.operator.DistinctOperator;
import com.iot.process.operator.InputParseOperator;
import com.iot.process.operator.SimpleGapOperator;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.core.fs.Path;


import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @description: 长期暂停事件检测
 * @author IoTBigdataBook
 * @Date 2023-01-12 14:51
 */

public class CEProcessFileOut {

    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        //准备kafka消费者配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        properties.put("group.id", "flink_process");

        //读取kafka topic中的数据，然后过滤其中重复的数据
        DataStream<String> kafkaStream = env.addSource(
                new FlinkKafkaConsumer<String>("first", new SimpleStringSchema(), properties)
        ).keyBy(data -> true).filter(new DistinctOperator());

//        //读取数据，然后过滤其中重复的数据（source.csv是input文件夹中两条船有序合并后的真实数据）
//        DataStream<String> filteredStream = env.readTextFile("data/source.csv")
//                .keyBy(data -> true)
//                .filter(new DistinctOperator());

        //将字符串映射成AISEvent对象, 然后分配笛卡尔坐标
        DataStream<AISEvent> mapedStream = kafkaStream.map(new InputParseOperator());
//                .map(new CoordinatesOperator());

        //设定watermark策略和提取事件时间
        DataStream<AISEvent> stream = mapedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AISEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<AISEvent>() {
                            @Override
                            public long extractTimestamp(AISEvent element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
        );

        SingleOutputStreamOperator<AISEvent> filterStream = stream.keyBy(data -> data.mmsi) //根据船号进行分区
                .window(EventTimeSessionWindows.withGap(Time.minutes(2))) //开窗（会话窗口）
                .process(new SimpleGapOperator()) //窗口处理
                .filter(data -> !data.isNoisy);// 去除噪声数据

        //执行程序
        //env.execute();

        // ---CEP---
        Pattern<AISEvent,AISEvent> patternTermStop = Pattern.<AISEvent>begin("start").where(new SimpleCondition<AISEvent>() {
            @Override
            public boolean filter(AISEvent aisEvent) throws Exception {

                return aisEvent.isGapEnd == true;
            }
        }).oneOrMore().next("second").where(new SimpleCondition<AISEvent>() {
            @Override
            public boolean filter(AISEvent aisEvent) throws Exception {
                return aisEvent.isPause == true;
            }
        }).next("third").where(new SimpleCondition<AISEvent>() {
            @Override
            public boolean filter(AISEvent aisEvent) throws Exception {
                return aisEvent.isGapStart = true;
            }
        }).within(Time.seconds(20));
        
        
        // ---CEP---
        Pattern<AISEvent,AISEvent> patternLongTermStop = Pattern.<AISEvent>begin("start").where(new SimpleCondition<AISEvent>() {
            @Override
            public boolean filter(AISEvent aisEvent) throws Exception {

                return aisEvent.isGapEnd == true;
            }
        });

        // ---CEP---
        Pattern<AISEvent,AISEvent> patternLongTermStop1 = Pattern.<AISEvent>begin("start").where(new SimpleCondition<AISEvent>() {
            @Override
            public boolean filter(AISEvent aisEvent) throws Exception {

                return aisEvent.isPause == false;
            }
        }).next("next").where(new SimpleCondition<AISEvent>() {
            @Override
            public boolean filter(AISEvent aisEvent) throws Exception {
                return aisEvent.isPause == true;
            }
        });
        
        //时间窗口的设定
        PatternStream<AISEvent> patternStream1 = CEP.pattern(stream,patternTermStop);
        PatternStream<AISEvent> patternStream2 = CEP.pattern(filterStream,patternTermStop);
        SingleOutputStreamOperator<String> output = patternStream2.select(
                new PatternSelectFunction<AISEvent, String>() {
                    @Override
                    public String select(Map<String, List<AISEvent>> map) throws Exception {
                        return "LongTermStop Event";
                    }
                }
        );

        output.print();
        String filePath = "//home//out.txt";
        output.addSink(StreamingFileSink.forRowFormat(new Path(filePath),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                //文件滚动间隔 每隔多久（指定）时间生成一个新文件
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(1))
                                //数据不活动时间 每隔多久（指定）未来活动数据，则将上一段时间（无数据时间段）也生成一个文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                // 文件最大容量
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build());

        //执行程序
        env.execute();
    }
}
