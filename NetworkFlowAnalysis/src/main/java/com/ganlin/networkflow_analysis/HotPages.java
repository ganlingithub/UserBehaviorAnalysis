package com.ganlin.networkflow_analysis;

import com.ganlin.networkflow_analysis.beans.ApacheLogEvent;
import com.ganlin.networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.regex.Pattern;

public class HotPages {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //读取文件转换成POJO类型
        //URL resource = HotPages.class.getResource("/apache.log");
        //DataStream<String> inputStream = env.readTextFile(resource.getPath());
        DataStream<String> inputStream = env.socketTextStream("localhost",7777);
        DataStream<ApacheLogEvent> dataStream = inputStream
                .map(
                        line ->{
                            String[] fields=line.split(" ");
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                            return new ApacheLogEvent(fields[0],fields[1],timestamp,fields[5],fields[6]);
                        }
                ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });
        dataStream.print("dataStream");
        //分组开窗聚合
        //定义一个侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};
        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream.filter(data -> "GET".equals(data.getMethod())) //过滤get请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex,data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)//按照url分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))//延迟时间允许1分钟
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());
        windowAggStream.print("agg");
        windowAggStream.getSideOutput(lateTag).print("late");
        //收集同一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd)
                .process(new TopNHotPages(3));
        resultStream.print();
        env.execute();
    }
    //实现自定义增量聚合函数
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent log, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long accumulator1, Long accumulator2) {
            return accumulator1+accumulator2;
        }
    }
    //自定义全窗口函数
    public static class PageCountResult implements WindowFunction<Long,PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String str, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) {
            String url = str;
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new PageViewCount(url,windowEnd,count));
        }
    }
    public static class TopNHotPages extends KeyedProcessFunction<Long,PageViewCount,String> {
        //定义topn大小
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        //定义列表状态，保存当前窗口内输出的PageViewCount
        //ListState<PageViewCount> pageViewCountListState;
        //保存到map中进行更新操作
        MapState<String,Long> pageViewCountMapState;
        @Override
        public void open(Configuration parameters) throws Exception {
            //pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-view-count-list", PageViewCount.class));
            pageViewCountMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String,Long>("page-view-count-map", String.class,Long.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> collector) throws Exception {
            //pageViewCountListState.add(value);
            pageViewCountMapState.put(value.getUrl(),value.getCount());
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1);
            //注册一个一分钟之后的定时器用来清空状态
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 60*1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //先判断是否到了窗口关闭清理时间，如果是直接清空状态返回
            if(timestamp == ctx.getCurrentKey()+60*1000L){
                pageViewCountMapState.clear();
                return;
            }

            //定时器触发，收集完成当前所有数据，排序输出
            //ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
            //pageViewCounts.sort(
            //        new Comparator<PageViewCount>() {
            //            @Override
            //            public int compare(PageViewCount o1, PageViewCount o2) {
            //                return o2.getCount().intValue() - o1.getCount().intValue();
            //            }
            //        }
            //);
            ArrayList<Map.Entry<String,Long>> pageViewCounts = Lists.newArrayList(pageViewCountMapState.entries().iterator());
            pageViewCounts.sort(
                    new Comparator<Map.Entry<String,Long>>() {
                        @Override
                        public int compare(Map.Entry<String,Long> o1, Map.Entry<String,Long> o2) {
                            if(o1.getValue()>o2.getValue()){
                                return -1;
                            }else if(o1.getValue()<o2.getValue()){
                                return 1;
                            }else{
                                return 0;
                            }
                        }
                    }
            );
            //将排名信息格式化成String,方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===========================");
            resultBuilder.append("窗口结束时间").append(new Timestamp(timestamp - 1)).append("\n");
            //遍历列表取topn
            //for (int i = 0; i < pageViewCounts.size() && i < topSize; ++i) {
            //    PageViewCount cur = pageViewCounts.get(i);
            //    resultBuilder.append("No ").append(i + 1).append(":")
            //            .append("URL = ").append(cur.getUrl())
            //            .append("热门度 = ").append(cur.getCount())
            //            .append("\n");
            //}
            for (int i = 0; i < pageViewCounts.size() && i < topSize; ++i) {
                Map.Entry<String, Long> cur = pageViewCounts.get(i);
                resultBuilder.append("No ").append(i + 1).append(":")
                        .append("URL = ").append(cur.getKey())
                        .append("热门度 = ").append(cur.getValue())
                        .append("\n");
            }
            resultBuilder.append("===========================\n\n");
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
            //pageViewCountListState.clear();

        }
    }
}
