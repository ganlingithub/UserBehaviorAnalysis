package com.ganlin.hotitems_analysis;

import com.ganlin.hotitems_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.omg.CORBA.Environment;

public class HotItemsWithSql {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据，创建DataStream数据流
        DataStream<String> inputStream = env.readTextFile("D:\\ganlin10\\Desktop\\waterdrop\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv");

        //3.转换成对应pojo类型，分配时间戳和watermark
        DataStream<UserBehavior> dataStream = inputStream
                .map(line->{
                    String[] fileds = line.split(",");
                    return new UserBehavior(new Long(fileds[0]),new Long(fileds[1]),new Integer(fileds[2]),new String(fileds[3]),new Long(fileds[4]));
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior userBehavior) {
                        return userBehavior.getTimestamp()*1000L;
                    }
                });
        //4.创建表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //5.将流转换成表
        Table dataTable = tableEnv.fromDataStream(dataStream,"itemId,behavior,timestamp.rowtime as ts");

        //6.分组开窗操作
        //table api
        Table windowAggTable = dataTable
                .filter("behavior = 'pv'")
                .window(Slide.over("1.hours").every("5.minutes")
                        .on("ts").as("w"))
                .groupBy("itemId,w")
                .select("itemId,w.end as windowEnd,itemId.count as cnt");

        //7.利用开窗函数对count值排序获取row no，得到top n
        //sql
        DataStream<Row> aggStream = tableEnv.toAppendStream(windowAggTable, Row.class);
        tableEnv.createTemporaryView("agg",aggStream,"itemId,windowEnd,cnt");
        Table resultTable = tableEnv.sqlQuery("select * from " +
                "(select *,ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "from agg ) temp where row_num<=5");
        tableEnv.toRetractStream(resultTable,Row.class).print();
        env.execute("hot items with sql job");
    }
}
