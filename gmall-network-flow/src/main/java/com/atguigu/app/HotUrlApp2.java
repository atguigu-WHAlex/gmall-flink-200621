package com.atguigu.app;

import com.atguigu.bean.ApacheLog;
import com.atguigu.bean.UrlViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

public class HotUrlApp2 {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本文件创建流,转换为JavaBean并提取时间戳
//        SingleOutputStreamOperator<ApacheLog> apachLogDS = env.readTextFile("input/apache.log")
        SingleOutputStreamOperator<ApacheLog> apachLogDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(" ");
                    SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    long time = sdf.parse(fields[3]).getTime();
                    return new ApacheLog(fields[0], fields[1], time, fields[5], fields[6]);
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLog>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLog element) {
                        return element.getEventTime();
                    }
                });

        OutputTag<ApacheLog> outputTag = new OutputTag<ApacheLog>("sideOutPut") {
        };

        //3.过滤数据,按照url分组,开窗,累加计算
        SingleOutputStreamOperator<UrlViewCount> aggregate = apachLogDS
                .filter(data -> "GET".equals(data.getMethod()))
                .keyBy(data -> data.getUrl())
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.seconds(60))
                .sideOutputLateData(outputTag)
                .aggregate(new UrlCountAggFunc(), new UrlCountWindowFunc());

        //4.按照窗口结束时间重新分组,计算组内排序
        SingleOutputStreamOperator<String> result = aggregate.keyBy(data -> data.getWindowEnd())
                .process(new UrlCountProcessFunc(5));

        //5.打印数据
        apachLogDS.print("apachLogDS");
        aggregate.print("aggregate");
        result.print("result");
        aggregate.getSideOutput(outputTag).print("side");

        //6.执行
        env.execute();

    }

    public static class UrlCountAggFunc implements AggregateFunction<ApacheLog, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLog value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class UrlCountWindowFunc implements WindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void apply(String url, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
            out.collect(new UrlViewCount(url, window.getEnd(), input.iterator().next()));
        }
    }

    public static class UrlCountProcessFunc extends KeyedProcessFunction<Long, UrlViewCount, String> {

        //定义TopSize属性
        private Integer topSize;

        public UrlCountProcessFunc() {
        }

        public UrlCountProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        //定义集合状态用于存放同一个窗口中的数据
        private MapState<String, UrlViewCount> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("map-state", String.class, UrlViewCount.class));
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {

            //将数据放置集合状态
            mapState.put(value.getUrl(),value);
            //注册定时器,用于处理状态中的数据
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
            //注册定时器,用于触发清空状态的
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 60000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            if (timestamp == ctx.getCurrentKey() + 60000L) {
                //清空状态
                mapState.clear();
                return;
            }

            //1.取出状态中的数据
            Iterator<Map.Entry<String, UrlViewCount>> iterator = mapState.entries().iterator();
            ArrayList<Map.Entry<String, UrlViewCount>> entries = Lists.newArrayList(iterator);

            //2.排序
            entries.sort(new Comparator<Map.Entry<String, UrlViewCount>>() {
                @Override
                public int compare(Map.Entry<String, UrlViewCount> o1, Map.Entry<String, UrlViewCount> o2) {
                    if (o1.getValue().getCount() > o2.getValue().getCount()) {
                        return -1;
                    } else if (o1.getValue().getCount() < o2.getValue().getCount()) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            StringBuilder sb = new StringBuilder();
            sb.append("======================\n");
            sb.append("当前窗口结束时间为:").append(new Timestamp(timestamp - 1L)).append("\n");

            //取前topSize条数据输出
            for (int i = 0; i < Math.min(topSize, entries.size()); i++) {
                //取出数据
                Map.Entry<String, UrlViewCount> entry = entries.get(i);
                sb.append("TOP ").append(i + 1);
                sb.append(" URL=").append(entry.getValue().getUrl());
                sb.append(" 页面热度=").append(entry.getValue().getCount());
                sb.append("\n");
            }
            sb.append("======================\n\n");

            //清空状态
//            listState.clear();

            Thread.sleep(1000);

            //输出数据
            out.collect(sb.toString());

        }
    }

}
