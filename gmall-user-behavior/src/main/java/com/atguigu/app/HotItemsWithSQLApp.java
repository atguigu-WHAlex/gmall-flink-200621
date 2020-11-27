package com.atguigu.app;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HotItemsWithSQLApp {

    public static void main(String[] args) throws Exception {

        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据
        DataStream<String> inputStream = env.readTextFile("input/UserBehavior.csv");

        //3.转换成Java Bean，提取时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        //4.创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        //5.把流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream, "itemId, behavior, timestamp.rowtime as ts");
        tableEnv.createTemporaryView("data_table", dataTable);

        //6.SQL实现
        String aggSql = "select itemId, count(itemId) as cnt, hop_end(ts,interval '5' minute, interval '1' hour) as windowEnd " +
                " from data_table where behavior = 'pv' " +
                " group by itemId, hop(ts,interval '5' minute, interval '1' hour)";

        Table aggSqlTable = tableEnv.sqlQuery(aggSql);
        tableEnv.createTemporaryView("agg", aggSqlTable);

        //7.基于聚合结果排序输出
        String topNSql = "select * from " +
                " (select *, row_number() over (partition by windowEnd order by cnt desc) as row_num" +
                "  from agg )" +
                " where row_num <= 5";
        Table resultTable = tableEnv.sqlQuery(topNSql);

        //8.转换为流进行输出
        tableEnv.toRetractStream(resultTable, Row.class).print();

        //9.执行任务
        env.execute("hot items with sql job");
    }
}
