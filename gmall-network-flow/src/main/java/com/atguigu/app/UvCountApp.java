package com.atguigu.app;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UvCount;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

public class UvCountApp {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.从文件读取数据创建流并转换为JavaBean同时提取事件时间
        SingleOutputStreamOperator<UserBehavior> userDS = env.readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] fileds = line.split(",");
                    return new UserBehavior(Long.parseLong(fileds[0]),
                            Long.parseLong(fileds[1]),
                            Integer.parseInt(fileds[2]),
                            fileds[3],
                            Long.parseLong(fileds[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.开窗一个小时
        SingleOutputStreamOperator<UvCount> result = userDS
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountAllWindowFunc());

        //4.打印
        result.print();

        //5.启动任务
        env.execute();

    }

    //自定义实现AllWindowFunction
    public static class UvCountAllWindowFunc implements AllWindowFunction<UserBehavior, UvCount, TimeWindow> {

        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> values, Collector<UvCount> out) throws Exception {

            //创建hashSet用于存放userID
            HashSet<Long> userIds = new HashSet<>();

            //遍历values
            Iterator<UserBehavior> iterator = values.iterator();
            while (iterator.hasNext()) {
                userIds.add(iterator.next().getUserId());
            }

            //输出数据
            String uv = "uv";
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = (long) userIds.size();
            out.collect(new UvCount(uv, windowEnd, count));

        }
    }

}
