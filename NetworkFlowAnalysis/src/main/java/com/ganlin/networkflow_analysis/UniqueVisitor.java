package com.ganlin.networkflow_analysis;

import com.ganlin.networkflow_analysis.beans.PageViewCount;
import com.ganlin.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.net.URL;
import java.util.HashSet;

public class UniqueVisitor {
    public static void main(String[] args) throws Exception{
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取数据，创建DataStream数据流
        URL resource = UniqueVisitor.class.getResource("/UserBehavior1.csv");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

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
        //4.分组开窗统计uv值
        SingleOutputStreamOperator<PageViewCount> uvStream = dataStream
                .filter(data -> "pv".equals(data.getBehavior())) //过滤pv行为
                .timeWindowAll(Time.hours(1))
                .apply(new UvCountResult());
        uvStream.print();
        env.execute("pv count job");
    }
    //实现自定义全窗口函数
    public static class UvCountResult implements AllWindowFunction<UserBehavior,PageViewCount, TimeWindow> {
        @Override
        public void apply(TimeWindow window, Iterable<UserBehavior> input, Collector<PageViewCount> out) {
            //定义set结构保存窗口中所有数据userId,自动输出
            HashSet<Long> uidSet = new HashSet<>();
            for(UserBehavior ub : input){
                uidSet.add(ub.getUserId());
            }
            out.collect(new PageViewCount("uv",window.getEnd(),(long)uidSet.size()));
        }
    }
}
