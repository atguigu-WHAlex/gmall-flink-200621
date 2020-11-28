package com.atguigu.app;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class OrderReceiptAppWithConnect {

    public static void main(String[] args) throws Exception {

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取文件数据创建流,转换为JavaBean,提取事件时间
        SingleOutputStreamOperator<OrderEvent> orderEventDS = env.readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new OrderEvent(Long.parseLong(fields[0]),
                            fields[1],
                            fields[2],
                            Long.parseLong(fields[3]));
                })
                .filter(data -> !"".equals(data.getTxId()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
                    @Override
                    public long extractAscendingTimestamp(OrderEvent element) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<ReceiptEvent> receiptEventDS = env.readTextFile("input/ReceiptLog.csv")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(fields[0], fields[1], Long.parseLong(fields[2]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //3.按照流水ID分组之后进行Connect,再做后续处理
        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> result = orderEventDS.keyBy(data -> data.getTxId())
                .connect(receiptEventDS.keyBy(data -> data.getTxId()))
                .process(new OrderPayReceiptCoProcessFunc());

        //4.打印数据
        result.print("payAndReceipt");
        result.getSideOutput(new OutputTag<String>("payButNoReceipt") {
        }).print("payButNoReceipt");
        result.getSideOutput(new OutputTag<String>("receiptButNoPay") {
        }).print("receiptButNoPay");

        //5.任务执行
        env.execute();

    }

    public static class OrderPayReceiptCoProcessFunc extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {

        //定义状态
        private ValueState<OrderEvent> orderEventValueState;
        private ValueState<ReceiptEvent> receiptEventValueState;
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            orderEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("order-state", OrderEvent.class));
            receiptEventValueState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt-state", ReceiptEvent.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state", Long.class));
        }

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {

            //判断receiptEventValueState状态是否为Null
            if (receiptEventValueState.value() == null) {

                //到账数据没有到达
                orderEventValueState.update(value);

                //注册5秒后的定时器
                long ts = (value.getEventTime() + 5) * 1000L;

                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);

            } else {
                //到账数据已经到达
                //输出数据
                out.collect(new Tuple2<>(value, receiptEventValueState.value()));
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsState.value());
                //清空状态
                orderEventValueState.clear();
                receiptEventValueState.clear();
                tsState.clear();
            }

        }

        @Override
        public void processElement2(ReceiptEvent value, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {

            //判断receiptEventValueState状态是否为Null
            if (orderEventValueState.value() == null) {

                //支付数据没有到达
                receiptEventValueState.update(value);

                //注册5秒后的定时器
                long ts = (value.getTimestamp() + 3) * 1000L;

                ctx.timerService().registerEventTimeTimer(ts);
                tsState.update(ts);

            } else {
                //支付数据已经到达
                //输出数据
                out.collect(new Tuple2<>(orderEventValueState.value(), value));
                //删除定时器
                ctx.timerService().deleteEventTimeTimer(tsState.value());
                //清空状态
                orderEventValueState.clear();
                receiptEventValueState.clear();
                tsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {

            //判断其中一个状态
            if (orderEventValueState.value() != null) {
                //只有支付没有到账数据
                ctx.output(new OutputTag<String>("payButNoReceipt") {
                }, orderEventValueState.value().getTxId() + "只有支付没有到账！");
            } else {
                //只有到账没有支付数据
                ctx.output(new OutputTag<String>("receiptButNoPay") {
                }, receiptEventValueState.value().getTxId() + "只有到账没有支付！");
            }

            //清空状态
            orderEventValueState.clear();
            receiptEventValueState.clear();
            tsState.clear();
        }
    }

}
