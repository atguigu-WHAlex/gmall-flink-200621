package com.atguigu.app;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderTimeoutAppWithState {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文件数据创建流,转换为JavaBean,提取事件时间
//        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        //3.按照订单ID分组
        SingleOutputStreamOperator<OrderResult> result = orderEventDS.keyBy(data -> data.getOrderId())
                .process(new OrderTimeOutProcessFunc());

        //4.打印
        result.print("payed");
        result.getSideOutput(new OutputTag<OrderResult>("payed timeout") {
        }).print("payed timeout");
        result.getSideOutput(new OutputTag<OrderResult>("pay timeout") {
        }).print("pay timeout");

        //5.执行
        env.execute();

    }

    public static class OrderTimeOutProcessFunc extends KeyedProcessFunction<Long, OrderEvent, OrderResult> {

        //定义状态
        private ValueState<Boolean> isCreateState;
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            isCreateState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created", Boolean.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<OrderResult> out) throws Exception {

            //判断事件类型
            if ("create".equals(value.getEventType())) {
                //来的是创建订单事件
                isCreateState.update(true);
                //注册定时器
                long ts = (value.getEventTime() + 900) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                //更新时间状态
                tsState.update(ts);
            } else if ("pay".equals(value.getEventType())) {

                //来的是支付事件,判断创建状态
                if (isCreateState.value() != null) {
                    //正常支付的订单
                    out.collect(new OrderResult(value.getOrderId(), "payed"));
                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(tsState.value());
                    //清空状态
                    isCreateState.clear();
                    tsState.clear();
                } else {
                    ctx.output(new OutputTag<OrderResult>("payed timeout") {
                               },
                            new OrderResult(value.getOrderId(), "payed timeout"));
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            //定时器触发,说明订单超时支付了
            ctx.output(new OutputTag<OrderResult>("pay timeout") {
                       },
                    new OrderResult(ctx.getCurrentKey(), "pay timeout"));

            //清空状态
            isCreateState.clear();
            tsState.clear();
        }
    }

}
