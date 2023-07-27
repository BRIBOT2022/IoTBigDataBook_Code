package com.iot.process.operator;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * 过滤Source中重复的数据
 * @author IoTBigdataBook
 * @create 2023-01-07 16:26
 */
public class DistinctOperator extends RichFilterFunction<String> {
    //定义状态，用于获取上一行的数据
    private ValueState<String> preLineState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //从运行时上下文中获取状态
        preLineState = getRuntimeContext().getState(
                new ValueStateDescriptor<String>("pre-line", String.class)
        );
    }

    @Override
    public boolean filter(String value) throws Exception {
        //从状态中获取先前的事件
        String preEvent = preLineState.value();
        if (preEvent != null) {
            //过滤重复的事件
            if (value.equals(preEvent)) {
                return false;
            } else {
                preLineState.update(value);
            }
        } else {
            //第一个事件
            preLineState.update(value);
        }
        return true;
    }
}
