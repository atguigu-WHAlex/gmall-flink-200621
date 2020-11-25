package com.atguigu.app;

import com.atguigu.bean.ItemCount;
import com.atguigu.bean.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

public class HotItemApp {

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

        //3.按照"pv"过滤,按照itemID分组,开窗,计算数据
        SingleOutputStreamOperator<ItemCount> itemCountDS = userDS
                .filter(data -> "pv".equals(data.getBehavior()))
                .keyBy(data -> data.getItemId())
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new ItemCountAggFunc(), new ItemCountWindowFunc());

        //4.按照windowEnd分组,排序输出
        SingleOutputStreamOperator<String> result = itemCountDS.keyBy("windowEnd")
                .process(new TopNItemIdCountProcessFunc(5));

        //5.打印结果
        result.print();

        //6.执行任务
        env.execute();
    }


    //自定义增量聚合函数
    public static class ItemCountAggFunc implements AggregateFunction<UserBehavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
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

    //自定义window函数
    public static class ItemCountWindowFunc implements WindowFunction<Long, ItemCount, Long, TimeWindow> {

        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out) throws Exception {
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemCount(itemId, windowEnd, count));
        }
    }

    //自定排序KeyedProcessFunction
    public static class TopNItemIdCountProcessFunc extends KeyedProcessFunction<Tuple, ItemCount, String> {

        //TopN属性
        private Integer topSize;

        public TopNItemIdCountProcessFunc() {
        }

        public TopNItemIdCountProcessFunc(Integer topSize) {
            this.topSize = topSize;
        }

        //定义ListState同于存放相同Key[windowEnd]的数据
        private ListState<ItemCount> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(new ListStateDescriptor<ItemCount>("list-state", ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            //每来一条数据,将数据存入集合状态
            listState.add(value);
            //注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //取出状态中的所有数据
            Iterator<ItemCount> iterator = listState.get().iterator();
            ArrayList<ItemCount> itemCounts = Lists.newArrayList(iterator);

            //排序
            itemCounts.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    if (o1.getCount() > o2.getCount()) {
                        return -1;
                    } else if (o1.getCount() < o2.getCount()) {
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
            for (int i = 0; i < Math.min(topSize, itemCounts.size()); i++) {
                //取出数据
                ItemCount itemCount = itemCounts.get(i);
                sb.append("TOP ").append(i + 1);
                sb.append(" ItemId=").append(itemCount.getItemId());
                sb.append(" 商品热度=").append(itemCount.getCount());
                sb.append("\n");
            }
            sb.append("======================\n\n");

            //清空状态
            listState.clear();

            Thread.sleep(1000);

            //输出数据
            out.collect(sb.toString());
        }
    }

}
