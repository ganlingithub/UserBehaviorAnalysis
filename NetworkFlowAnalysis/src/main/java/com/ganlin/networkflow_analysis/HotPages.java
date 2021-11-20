package com.ganlin.networkflow_analysis;

import com.ganlin.networkflow_analysis.beans.ApacheLogEvent;
import com.ganlin.networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;

public class HotPages {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //读取文件转换成POJO类型
        URL resource = HotPages.class.getResource("/apache.log");
        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<ApacheLogEvent> dataStream = inputStream
                .map(
                        line ->{
                            String[] fields=line.split(" ");
                            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
                            return new ApacheLogEvent(fields[0],fields[1],timestamp,fields[5],fields[6]);
                        }
                ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.minutes(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent element) {
                        return element.getTimestamp();
                    }
                });
        //分组开窗聚合
        DataStream<PageViewCount> windowAggStream = dataStream.filter(data -> "GET".equals(data.getMethod())) //过滤get请求
                .keyBy(ApacheLogEvent::getUrl)//按照url分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new PageCountAgg(), new PageCountResult());
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
        ListState<PageViewCount> pageViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("page-view-count-list", PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<String> collector) throws Exception {
            pageViewCountListState.add(value);
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，收集完成当前所有数据，排序输出
            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
            pageViewCounts.sort(
                    new Comparator<PageViewCount>() {
                        @Override
                        public int compare(PageViewCount o1, PageViewCount o2) {
                            return o2.getCount().intValue() - o1.getCount().intValue();
                        }
                    }
            );
            //将排名信息格式化成String,方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("===========================");
            resultBuilder.append("窗口结束时间").append(new Timestamp(timestamp - 1)).append("\n");
            //遍历列表取topn
            for (int i = 0; i < pageViewCounts.size() && i < topSize; ++i) {
                PageViewCount cur = pageViewCounts.get(i);
                resultBuilder.append("No ").append(i + 1).append(":")
                        .append("URL = ").append(cur.getUrl())
                        .append("热门度 = ").append(cur.getCount())
                        .append("\n");
            }
            resultBuilder.append("===========================\n\n");
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
