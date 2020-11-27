package com.atguigu.app;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UvCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

public class UvCountWithBloomFilterApp {

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
                .trigger(new MyTrigger())
                .process(new UvWithBloomFilterWindowFunc());

        //4.打印
        result.print();

        //5.启动任务
        env.execute();

    }

    //自定义触发器,每来一条数据,触发一次计算并输出结果
    public static class MyTrigger extends Trigger<UserBehavior, TimeWindow> {

        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UvWithBloomFilterWindowFunc extends ProcessAllWindowFunction<UserBehavior, UvCount, TimeWindow> {

        //定义Redis连接
        Jedis jedis;

        //定义一个布隆过滤器
        MyBloomFilter myBloomFilter;

        //定义UvCountRedisKey  Hash
        String uvCountRedisKey;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("hadoop102", 6379);
            myBloomFilter = new MyBloomFilter(1L << 29);//64M
            uvCountRedisKey = "UvCount";
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> elements, Collector<UvCount> out) throws Exception {

            //1.获取窗口信息并指定UvCountRedisKey的Field,同时指定BitMap的RedisKey
            String windowEnd = new Timestamp(context.window().getEnd()).toString();
            String bitMapRedisKey = "UvBitMap:" + windowEnd;

            //2.判断当前的userID是否已经存在
            Long offset = myBloomFilter.hash(elements.iterator().next().getUserId() + "");
            Boolean exist = jedis.getbit(bitMapRedisKey, offset);

            //3.如果不存在,向Redis中累加数据,并将BitMap中对应位置改为true
            if (!exist) {
                jedis.hincrBy(uvCountRedisKey, windowEnd, 1L);
                jedis.setbit(bitMapRedisKey, offset, true);
            }

            //4.取出Redis中对应的Count值,发送
            long count = Long.parseLong(jedis.hget(uvCountRedisKey, windowEnd));

            //5.发送数据
            out.collect(new UvCount("uv", windowEnd, count));

        }


        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }

    public static class MyBloomFilter {

        //定义布隆过滤器的容量,此时需要2的整次幂
        private Long cap;

        public MyBloomFilter() {
        }

        public MyBloomFilter(Long cap) {
            this.cap = cap;
        }

        public Long hash(String value) {

            int result = 0;

            for (char c : value.toCharArray()) {
                result = result * 31 + c;
            }

            //位与运算
            return result & (cap - 1);

        }
    }


}
