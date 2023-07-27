package com.iot.collect.simulator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @description: TODO
 * @Author Juniors Lee
 * @Date 2022-09-26 13:34
 */
public class CSVDataSimulator {

    public static void main(String[] args) throws InterruptedException, IOException {

        List<String> dataList = new ArrayList<>();

        dataList.add("55,1521683786,184.47958333333332,37.633616666666654,5.4607,0\n");
        dataList.add("54,1521689549,169.62198333333336,37.54343333333335,5.5264,0\n");
        dataList.add("96,1521697266,128.73608333333333,37.349066666666666,4.387,0\n");
        dataList.add("60,1521642061,106.2317,38.36993333333333,5.6648,0\n");
        dataList.add("36,1521660982,151.81826666666666,38.02538333333335,5.5083,0\n");
        dataList.add("14,1521676796,167.29856666666666,37.735516666666655,5.6668,0\n");

        File csvFile = new File("src/main/java/com/iot/collect/data/sea.csv");

        //模拟出来的数据追加写入
        FileOutputStream stream = new FileOutputStream(csvFile,true);

        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            String data = dataList.get(random.nextInt(5));
            data = data.substring(0,3) + System.currentTimeMillis() + data.substring(13);
            stream.write(data.getBytes(StandardCharsets.UTF_8));
            System.out.println(data);
            Thread.sleep(1000);
        }
    }
}
