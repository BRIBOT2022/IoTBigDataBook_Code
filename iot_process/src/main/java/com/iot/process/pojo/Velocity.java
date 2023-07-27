package com.iot.process.pojo;

import java.io.Serializable;

/**
 * @author IoTBigdataBook
 * @create 2022-12-18 17:58
 */
public class Velocity {
    public double speed;//速率
    public double angle;//角度

    @Override
    public String toString() {
        return "(" + "speed: " + speed + ", angle: " + angle + ")";
    }
}
