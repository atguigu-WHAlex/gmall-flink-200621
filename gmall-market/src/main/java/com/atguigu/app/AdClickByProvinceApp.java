package com.atguigu.app;

import com.atguigu.bean.AdClickEvent;
import com.atguigu.bean.AdCountByProvince;
import com.atguigu.bean.BlackListWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

public class AdClickByProvinceApp {

    public static void main(String[] args) throws Exception {

        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流转换为JavaBean,并指定时间戳字段
        SingleOutputStreamOperator<AdClickEvent> adClickDS = env.readTextFile("input/AdClickLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new AdClickEvent(Long.parseLong(fields[0]),
                            Long.parseLong(fields[1]),
                            fields[2],
                            fields[3],
                            Long.parseLong(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.根据点击量进行数据过滤(单日某个用户点击某个广告超过100次,则加入黑名单)
        SingleOutputStreamOperator<AdClickEvent> filterByClickCount = adClickDS
                .keyBy("userId", "adId")
                .process(new AdClickKeyProcessFunc(100L));

        //4.按照省份分组,开窗,计算各个省份广告点击总数
        SingleOutputStreamOperator<AdCountByProvince> result = filterByClickCount
                .keyBy(data -> data.getProvince())
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AdClickAgg(), new AdClickWindowFunc());

        //获取侧输出流
        DataStream<BlackListWarning> sideOutput = filterByClickCount.getSideOutput(new OutputTag<BlackListWarning>("outPut") {
        });

        //5.打印数据
        result.print();
        sideOutput.print("sideOutPut");

        //6.启动任务
        env.execute();

    }


    public static class AdClickAgg implements AggregateFunction<AdClickEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent value, Long accumulator) {
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

    public static class AdClickWindowFunc implements WindowFunction<Long, AdCountByProvince, String, TimeWindow> {

        @Override
        public void apply(String province, TimeWindow window, Iterable<Long> input, Collector<AdCountByProvince> out) throws Exception {

            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();

            out.collect(new AdCountByProvince(province, windowEnd, count));

        }
    }

    public static class AdClickKeyProcessFunc extends KeyedProcessFunction<Tuple, AdClickEvent, AdClickEvent> {
        //定义单日单人点击某个广告上界
        private Long maxClick;

        public AdClickKeyProcessFunc() {
        }

        public AdClickKeyProcessFunc(Long maxClick) {
            this.maxClick = maxClick;
        }

        //定义状态信息
        private ValueState<Long> countState;
        private ValueState<Boolean> isBlackList;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count-state", Long.class));
            isBlackList = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-black-list", Boolean.class));
        }

        @Override
        public void processElement(AdClickEvent value, Context ctx, Collector<AdClickEvent> out) throws Exception {

            //获取状态中的数据
            Long count = countState.value();

            //判断是否是第一条数据
            if (count == null) {

                //如果是第一条数据
                countState.update(1L);

                //定义定时器,用于清空状态
                long ts = (value.getTimestamp() / (60 * 60 * 24) + 1) * (24 * 60 * 60 * 1000L) - (8 * 60 * 60 * 1000L);
                System.out.println(new Timestamp(ts));
                ctx.timerService().registerEventTimeTimer(ts);

            } else {
                //如果不是第一条数据,更新状态为之前的数据+1
                long curClickCount = count + 1L;
                countState.update(curClickCount);

                if (curClickCount >= maxClick) {

                    //判断是否已经被拉黑
                    if (isBlackList.value() == null) {
                        //超过单日点击次数,将数据输出到侧输出流
                        ctx.output(new OutputTag<BlackListWarning>("outPut") {
                                   },
                                new BlackListWarning(value.getUserId(), value.getAdId(), "点击次数超过" + maxClick + "次！"));

                        //更新状态为true
                        isBlackList.update(true);
                    }
                    return;
                }
            }

            //将数据写入主流
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            countState.clear();
            isBlackList.clear();
        }
    }

}
