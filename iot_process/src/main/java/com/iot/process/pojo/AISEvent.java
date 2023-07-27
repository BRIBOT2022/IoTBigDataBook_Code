package com.iot.process.pojo;

import org.apache.flink.api.java.tuple.Tuple3;

import java.sql.Timestamp;

/**
 * @author IoTBigdataBook
 * @create 2022-12-18 11:49
 */
public class AISEvent {
    public long previousTS;// 前一个事件的时间戳
    public long timestamp;// 当前事件的时间戳
    public String mmsi;// 船号
    public double latitude;// 纬度
    public double longitude;// 经度
    public Tuple3<Double, Double, Double> coordinate;//笛卡尔坐标
    // 瞬时速度（由于这里计算出的是平均速度，所以没有用到论文中提到的instantSpeed属性）
    // public double instantSpeed;
    public Velocity velocity;// 速度矢量（基于角度和速度属性的Velocity类）
    public boolean isNoisy;// 是否为噪声
    public boolean isPause;// 是否暂停
    public boolean isTurn;// 是否转向
    public boolean isSpeedChange;// 是否变速
    public boolean isGapStart;// 是否为窗口之间的间隔开始
    public boolean isGapEnd;// 是否为窗口之间的间隔结束

    public AISEvent() {
        velocity = new Velocity();
    }

    //计算当前AISEvent对象与其他AISEvent的距离
    public double computeDistance(AISEvent other) {
        return haversineToDistance(other.latitude, other.longitude, latitude, longitude);
    }

    //计算当前AISEvent对象的速度（平均速度？）
    public double computeSpeed(AISEvent previous) {
        //获取当前事件的船舶与上一个事件中船舶的距离（单位m）
        double distance = computeDistance(previous);
        //获取上一个事件的时间戳，并为当前属性赋值
        this.previousTS = previous.timestamp;
        //时间差（单位秒）
        double interval = (timestamp - previousTS) / 1000;
        //计算速度（将单位 m/s换算为 km/h）
        double speed = (distance/ interval) * 3.6;
        this.velocity.speed = speed;
        return speed;
    }

    //计算当前AISEvent对象的转向
    public double computeBearing(AISEvent previous) {
        double bearing = getAngle(previous.latitude, previous.longitude, latitude, longitude);
        this.velocity.angle = bearing;
        return bearing;
    }

    //直接根据两个位置的经纬度，计算两个位置的距离
    //haversine公式参考地址：https://blog.csdn.net/spatial_coder/article/details/116605509
    public double haversineToDistance(double lat1, double lon1, double lat2, double lon2) {
        //将角度换算为弧度制
        lat1 = Math.toRadians(lat1);
        lon1 = Math.toRadians(lon1);
        lat2 = Math.toRadians(lat2);
        lon2 = Math.toRadians(lon2);
        //计算经纬度距离
        double latProcess = Math.sin((lat2 - lat1) / 2) * Math.sin((lat2 - lat1) / 2);
        double lonProcess = Math.sin((lon2 - lon1) / 2) * Math.sin((lon2 - lon1) / 2);
        double process1 = latProcess + Math.cos(lat2) * Math.cos(lat1) * lonProcess;
        double process2 = Math.sqrt(process1);
        double process3 = Math.asin(process2);
        return 2 * 6367 * 1000 * process3;//单位 m
    }

    //根据两个位置的经纬度，计算两个位置的角度
    //原理参考地址：http://www.movable-type.co.uk/scripts/latlong.html
    //代码参考地址：https://blog.csdn.net/xiaobai091220106/article/details/50879414
    public double getAngle(double lat1, double lon1, double lat2, double lon2) {
        //先将角度换算为弧度制
        lat1 = Math.toRadians(lat1);
        lon1 = Math.toRadians(lon1);
        lat2 = Math.toRadians(lat2);
        lon2 = Math.toRadians(lon2);
        double y = Math.sin(lon2 - lon1) * Math.cos(lat2);
        double x = Math.cos(lat1) * Math.sin(lat2) - Math.sin(lat1) * Math.cos(lat2) * Math.cos(lon2-lon1);
        double brng = Math.atan2(y, x);
        //将弧度转换为角度
        brng = Math.toDegrees(brng);
        return brng < 0 ? brng + 360 : brng;
    }

    @Override
    public String toString() {
        return "AISEvent{" +
                "time=" + new Timestamp(timestamp) +
                ", mmsi='" + mmsi + '\'' +
                ", isGapStart=" + isGapStart +
                ", isGapEnd=" + isGapEnd +
                ", isPause=" + isPause +
                ", isSpeedChange=" + isSpeedChange +
                ", isTurn=" + isTurn +
                ", " + velocity +
                '}';
    }
}
