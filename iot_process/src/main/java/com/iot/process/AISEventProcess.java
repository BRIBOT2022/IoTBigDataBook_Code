package com.iot.process;

import com.iot.process.operator.DistinctOperator;
import com.iot.process.operator.InputParseOperator;
import com.iot.process.operator.SimpleGapOperator;
import com.iot.process.pojo.AISEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author IoTBigdataBook
 * @create 2023-01-18 17:38
 */
public class AISEventProcess {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 准备kafka消费者配置信息
        // 如果使用Kafka数据源，要先配置好Kafka集群，根据自己机器的IP地址，修改下面的配置
        // 如果使用Kafka作为数据源，可先通过AISProducer程序将source.csv中的数据发送到Kafka中
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        properties.put("group.id", "flink_process");
        // Kafka数据源：读取kafka topic中的数据
        DataStream<AISEvent> kafkaStream = env.addSource(
                new FlinkKafkaConsumer<String>("first", new SimpleStringSchema(), properties)
        ).keyBy(data -> true).filter(new DistinctOperator()) //过滤重复的数据
         .map(new InputParseOperator()); //将字符串映射成AISEvent对象

        // 文本文件数据源：读取source.csv中的数据（source.csv是raw文件夹中两条船有序合并后的真实数据）
//        DataStream<AISEvent> kafkaStream = env.readTextFile("data/source.csv")
//                .keyBy(data -> true).filter(new DistinctOperator()) //过滤重复的数据
//                .map(new InputParseOperator()); //将字符串映射成AISEvent对象

        // 设定watermark策略和提取事件时间
        DataStream<AISEvent> stream = kafkaStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AISEvent>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<AISEvent>() {
                                    @Override
                                    public long extractTimestamp(AISEvent element,
                                                                 long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));

        /* Tip：
         *  使用Kafka作为Source时，可能会出现数据丢失的情况
         *  以下代码可以查看每个窗口中到达的数据条数，以此来判断是否发送数据丢失的情况
         *  当使用以下注释的内容进行测试时，最好将下面Sink到文件的部分代码注释
         *  这里选择会话窗口的原因见论文
         */
//        stream.keyBy(data -> data.mmsi) // 根据船号进行分区
//                .window(EventTimeSessionWindows.withGap(Time.minutes(135))) // 开窗（会话窗口）
//                .aggregate(
//                        new AggregateFunction<AISEvent, Long, String>() {
//                            @Override
//                            public Long createAccumulator() {
//                                return 0L;
//                            }
//
//                            @Override
//                            public Long add(AISEvent value, Long accumulator) {
//                                return accumulator + 1;
//                            }
//
//                            @Override
//                            public String getResult(Long accumulator) {
//                                return accumulator.toString();
//                            }
//
//                            @Override
//                            public Long merge(Long a, Long b) {
//                                return null;
//                            }
//                        }
//                ).print();

        //指定将数据输出到文件的策略
//        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(
//                new Path("data/output"),
//                new SimpleStringEncoder<>("UTF-8")
//        ).withRollingPolicy(DefaultRollingPolicy.builder()
//                //指定文件最大的容量
//                .withMaxPartSize(1024 * 1024 * 1024)
//                //设置隔多长时间间隔滚动一次,开启一个新的文件
//                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//                //设置当前不活跃的间隔时间
//                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//                .build()
//        ).build();
//
//        stream.keyBy(data -> data.mmsi) //根据船号进行分区
//                .window(EventTimeSessionWindows.withGap(Time.minutes(135))) //开窗（会话窗口）
//                .process(new SimpleGapOperator()) //窗口处理
//                .filter(data -> !data.isNoisy) // 去除噪声数据
//                .map(data -> data.toString()) //将数据转换成字符串
//                .addSink(streamingFileSink); //将数据输出到文本文件

        stream.keyBy(data -> data.mmsi) // 根据船号进行分区
                .window(EventTimeSessionWindows.withGap(Time.minutes(135))) // 开窗（会话窗口）
                .process(new SimpleGapOperator()) // 窗口处理
                .filter(data -> !data.isNoisy) // 去除噪声数据
                .print(); // 打印输出结果


        //执行程序
        env.execute();
    }
}
