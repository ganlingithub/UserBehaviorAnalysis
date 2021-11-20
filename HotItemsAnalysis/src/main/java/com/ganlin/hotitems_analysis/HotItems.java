package com.ganlin.hotitems_analysis;

import com.ganlin.hotitems_analysis.beans.ItemViewCount;
import com.ganlin.hotitems_analysis.beans.UserBehavior;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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

public class HotItems {
    public static void main(String[] args) throws Exception{
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
        //4.分组开窗聚合，得到每个窗口内各个商品的count值
        DataStream<ItemViewCount> windowAggStream = dataStream
                .filter(data ->"pv".equals(data.getBehavior())) //过滤pv
                .keyBy("itemId")    //按商品id分组
                .timeWindow(Time.hours(1),Time.minutes(5)) //开滑动窗口
                .aggregate(new ItemViewCountAgg(),new WindowItemCountResult());
        //5.收集统一窗口的所有商品count数据,排序输出topn
        DataStream<String> resultStream = windowAggStream
                .keyBy("windowEnd")//按照窗口分组
                .process(new TopNHotItems(5));
        resultStream.print();
        env.execute("hot item analysis");
    }
    //实现自定义增量聚合函数
    public static class ItemViewCountAgg implements AggregateFunction<UserBehavior,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long accumulator) {
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
    public static class WindowItemCountResult implements WindowFunction<Long,ItemViewCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) {
            Long itemId = tuple.getField(0);
            Long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId,windowEnd,count));
        }
    }
    public static class TopNHotItems extends KeyedProcessFunction<Tuple,ItemViewCount,String>{
        //定义topn大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }
        //定义列表状态，保存当前窗口内输出的ItemViewCount
        ListState<ItemViewCount> itemViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("item-view-count-list",ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> collector) throws Exception {
            itemViewCountListState.add(value);
            ctx.timerService().registerProcessingTimeTimer(value.getWindowEnd()+1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，收集完成当前所有数据，排序输出
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(itemViewCountListState.get().iterator());
            itemViewCounts.sort(
                    new Comparator<ItemViewCount>() {
                        @Override
                        public int compare(ItemViewCount o1, ItemViewCount o2) {
                            return o2.getCount().intValue()-o1.getCount().intValue();
                        }
                    }
            );
            //将排名信息格式化成String,方便打印输出
            StringBuilder  resultBuilder = new StringBuilder();
            resultBuilder.append("===========================");
            resultBuilder.append("窗口结束时间").append(new Timestamp(timestamp-1)).append("\n");
            //遍历列表取topn
            for(int i=0;i<itemViewCounts.size()&&i<topSize;++i){
                ItemViewCount cur = itemViewCounts.get(i);
                resultBuilder.append("No ").append(i+1).append(":")
                        .append("商品ID = ").append(cur.getItemId())
                        .append("热门度 = ").append(cur.getCount())
                        .append("\n");
            }
            resultBuilder.append("===========================\n\n");
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());
        }
    }
}
