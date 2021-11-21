package com.ganlin.networkflow_analysis;

import com.ganlin.networkflow_analysis.beans.ApacheLogEvent;
import com.ganlin.networkflow_analysis.beans.PageViewCount;
import com.ganlin.networkflow_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.regex.Pattern;

public class PageView {
    public static void main(String[] args) throws Exception{
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据，创建DataStream数据流
        PageView.class.getResource("/UserBehavior.csv");
        DataStream<String> inputStream = env.readTextFile("D:\\ganlin10\\Desktop\\waterdrop\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv");

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
        //4.分组开窗聚合，得到每个窗口内各个商品的count值
        SingleOutputStreamOperator<Tuple2<String,Long>> pvResultStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) //过滤pv行为
                .map(new MapFunction<UserBehavior, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior userBehavior) throws Exception {
                        return new Tuple2<>("pv",1L);
                    }
                })
                .keyBy(0)//按照商品id分组
                .timeWindow(Time.hours(1))//开1小时滚动窗口
                .sum(1);
        pvResultStream.print();
        env.execute("pv count job");
    }
}
