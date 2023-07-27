package com.iot.process.operator;

import com.iot.process.pojo.AISEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 判断当前AISEvent对象是否是 GapStart 或 GapEnd, 并设置属性
 * 然后判断船舶每个轨迹点的事件：isPause, isSpeedChange, isTurn, isNoisy
 * 事件的判断依据见论文
 * @author IoTBigdataBook
 * @create 2022-12-28 18:02
 */
public class SimpleGapOperator extends ProcessWindowFunction<AISEvent, AISEvent, String, TimeWindow> {
    //定义上一个事件的状态
    private ValueState<AISEvent> preEventState;

    @Override
    public void open(Configuration parameters) throws Exception {
        //从运行时环境中获取状态
        preEventState = getRuntimeContext().getState(
                new ValueStateDescriptor<AISEvent>("pre-event", AISEvent.class)
        );
    }

    @Override
    public void process(String mmsi,
                        ProcessWindowFunction<AISEvent, AISEvent, String, TimeWindow>.Context context,
                        Iterable<AISEvent> elements,
                        Collector<AISEvent> out)
            throws Exception {
        //获取当前窗口中所有事件的数量
        int len = 0;
        for (AISEvent element : elements) {
            ++len;
        }
        //index用于标识当前元素是窗口中的第几个元素
        int index = 1;
        //判断事件是否是GapEnd或GapStart, 并设置事件中的属性
        for (AISEvent event : elements) {
            if (index == 1) {
                //将窗口的第一个元素定义为GapEnd
                event.isGapEnd = true;
            } else if (index == len) {
                //将窗口的最后一个元素定义为GapStart
                event.isGapStart = true;
            }
            ++index;
        }

        //单位km/h（knot是衡量船舶在海上速度的单位，意为“海里/秒”，1海里=1.852千米）
        double knot = 1.852;
        //判断是否发生暂停或变速事件, 同时判断是否是噪声事件, 并设置事件中的属性
        for (AISEvent value : elements) {
            if (!value.isGapEnd) {
                //从状态中获取上一个事件
                AISEvent preEvent = preEventState.value();
                //计算当前事件中船舶的速度和角度
                value.computeSpeed(preEvent);
                value.computeBearing(preEvent);
                //计算船舶的速度差和方位差
                double speedDifference = value.velocity.speed - preEvent.velocity.speed;
                double bearingDifference = value.velocity.angle - preEvent.velocity.angle;

                //判断是否发生暂停事件
                if (value.velocity.speed < knot) {
                    value.isPause = true;
                }
                //判断是否发生变速事件
                if (Math.abs(speedDifference) > 0.25 * knot) {
                    value.isSpeedChange = true;
                }
                //判断是否发生转向事件
                if (Math.abs(bearingDifference) > 15) {
                    value.isTurn = true;
                }
                //判断是否发生噪声事件
                if (value.velocity.speed > 30 * knot || Math.abs(bearingDifference) > 60) {
                    value.isNoisy = true;
                }
            }
            //更新上一个事件的值，并输出当前事件
            preEventState.update(value);
            out.collect(value);
        }
        //清空状态
        preEventState.clear();
    }
}
