package com.atguigu.app;

import com.atguigu.bean.ChannelBehaviorCount;
import com.atguigu.bean.MarketUserBehavior;
import com.atguigu.source.MarketBehaviorSource;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class MarketByChannelApp {

    public static void main(String[] args) throws Exception {

        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取自定义数据源数据
        DataStreamSource<MarketUserBehavior> marketUserDS = env.addSource(new MarketBehaviorSource());

        //3.过滤卸载数据,按照渠道和行为做分组,开窗
        SingleOutputStreamOperator<ChannelBehaviorCount> result = marketUserDS
                .filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new MarketAggFunc(), new MarketWindowFunc());

        //4.打印
        result.print();

        //5.执行任务
        env.execute();

    }

    public static class MarketAggFunc implements AggregateFunction<MarketUserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(MarketUserBehavior value, Long accumulator) {
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

    public static class MarketWindowFunc implements WindowFunction<Long, ChannelBehaviorCount, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ChannelBehaviorCount> out) throws Exception {

            String channel = tuple.getField(0);
            String behavior = tuple.getField(1);
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();

            out.collect(new ChannelBehaviorCount(channel, behavior, windowEnd, count));

        }
    }

}
