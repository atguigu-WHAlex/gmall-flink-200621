package com.atguigu.app;

import com.atguigu.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class LoginFailApp {

    public static void main(String[] args) throws Exception {

        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文本数据创建流,转换为JavaBean,提取时间戳
        SingleOutputStreamOperator<LoginEvent> loginEventDS = env.readTextFile("input/LoginLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new LoginEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(LoginEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.按照userId进行分组
        SingleOutputStreamOperator<String> result = loginEventDS
                .keyBy(data -> data.getUserId())
                .process(new LoginFailKeyProcessFunc());

        //4.打印
        result.print();

        //5.执行
        env.execute();


    }

    public static class LoginFailKeyProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {

        //定义状态数据
        private ListState<LoginEvent> listState;
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("list-state", LoginEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {

            //取出状态数据
            Iterable<LoginEvent> loginEvents = listState.get();

            //判断是否为空,确定是否为第一条失败数据
            if (!loginEvents.iterator().hasNext()) {

                //第一条数据,则判断当前数据是否为登录失败
                if ("fail".equals(value.getEventType())) {

                    //将当前数据放置状态中,注册2s后的定时器
                    listState.add(value);

                    long ts = (value.getTimestamp() + 2) * 1000L;
                    ctx.timerService().registerEventTimeTimer(ts);

                    tsState.update(ts);
                }
            } else {

                //不是第一条数据,并且是失败数据
                if ("fail".equals(value.getEventType())) {
                    listState.add(value);
                } else {
                    //不是第一条数据,并且是成功数据,删除定时器
                    ctx.timerService().deleteEventTimeTimer(tsState.value());
                    //清空状态
                    listState.clear();
                    tsState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态中的数据
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(listState.get().iterator());
            int size = loginEvents.size();

            //如果集合中数据大于等于2,则输出报警信息
            if (size >= 2) {
                LoginEvent firstFail = loginEvents.get(0);
                LoginEvent lastFail = loginEvents.get(size - 1);
                out.collect(ctx.getCurrentKey() +
                        "在" + firstFail.getTimestamp() +
                        "到" + lastFail.getTimestamp() +
                        "之间登录失败" + size + "次！");
            }

            //清空状态
            listState.clear();
            tsState.clear();

        }
    }

}
