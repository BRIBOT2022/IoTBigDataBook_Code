package com.iot.process.operator;

import com.iot.process.pojo.AISEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * 将从Source中读取的字符串映射为AISEvent对象
 * 数据格式: "1614960620.0,220317000,54.751307,10.671393"
 * @author IoTBigdataBook
 * @create 2023-01-04 21:23
 */
public class InputParseOperator implements MapFunction<String, AISEvent> {
    @Override
    public AISEvent map(String value) throws Exception {
        String[] fields = value.split(",");
        AISEvent aisEvent = new AISEvent();
        // 原始文件中的时间戳是浮点型，所以要先将浮点型时间戳转换为long型
        // 由于这里的时间戳是10位的，因此需要在末尾加上三个0，转换为13位的时间戳
        // 例："1614960620" + "000" --> "1614960620000"
        long timestamp = (long) Double.parseDouble(fields[0]);
        if (String.valueOf(timestamp).length() == 10) {
            aisEvent.timestamp = timestamp * 1000;
        } else {
            aisEvent.timestamp = timestamp;
        }
        aisEvent.mmsi = fields[1].trim();
        aisEvent.latitude = Double.parseDouble(fields[2]);
        aisEvent.longitude = Double.parseDouble(fields[3]);
        return aisEvent;
    }
}
