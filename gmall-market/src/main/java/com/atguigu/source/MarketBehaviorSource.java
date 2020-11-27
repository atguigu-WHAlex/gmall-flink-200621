package com.atguigu.source;

import com.atguigu.bean.MarketUserBehavior;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MarketBehaviorSource implements ParallelSourceFunction<MarketUserBehavior> {

    // 是否运行的标识位
    private Boolean running = true;

    // 定义用户行为和推广渠道的集合
    private List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
    private List<String> channelList = Arrays.asList("app store", "wechat", "weibo", "tieba");

    // 定义随机数发生器
    private Random random = new Random();

    @Override
    public void run(SourceContext<MarketUserBehavior> ctx) throws Exception {
        while (running) {

            // 随机生成所有字段
            Long id = random.nextLong();
            String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
            String channel = channelList.get(random.nextInt(channelList.size()));
            Long timestamp = System.currentTimeMillis();

            // 发出数据
            ctx.collect(new MarketUserBehavior(id, behavior, channel, timestamp));
            Thread.sleep(100L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
