package com.ganlin.market_analysis;

import com.ganlin.market_analysis.beans.AdClickEvent;
import com.ganlin.market_analysis.beans.AdCountViewByProvince;
import com.ganlin.market_analysis.beans.BlackListUserWarning;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
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
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.sql.Timestamp;

public class AdStatisticsByProvice {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1.从文件中读取数据
        URL resource = AdStatisticsByProvice.class.getResource("/AdClickLog.csv");
        DataStream<AdClickEvent> adClickEventStream = env.readTextFile(resource.getPath())
                .map(
                        line ->{
                            String[] fields = line.split(",");
                            return new AdClickEvent(new Long(fields[0]),new Long(fields[1]),fields[2],fields[3],new Long(fields[4]));
                        }
                )
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickEvent>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickEvent adClickEvent) {
                        return adClickEvent.getTimestamp()*1000L;
                    }
                });
        //2.对同一个用户点击同一个广告的行为进行检测
        SingleOutputStreamOperator<AdClickEvent> filterAdClickStream = adClickEventStream
                .keyBy("userId", "adId")
                .process(new FilterBlackListUser(100));
        filterAdClickStream.getSideOutput(new OutputTag<BlackListUserWarning>("blacklist"){}).print("blacklist-user");
        //3.基于省份分组,开窗聚合
        SingleOutputStreamOperator<AdCountViewByProvince> adCountResultStream = adClickEventStream.keyBy(AdClickEvent::getProvince)
                .timeWindow(Time.hours(1), Time.minutes(5)) //定义滑窗，5min输出一次
                .aggregate(new AdCountAgg(), new AdCountResult());
        adCountResultStream.print();
        env.execute("ad count by province jon");
    }
    public static class AdCountAgg implements AggregateFunction<AdClickEvent,Long,Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdClickEvent adClickEvent, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }
    public static class AdCountResult implements WindowFunction<Long, AdCountViewByProvince, String, TimeWindow> {
        @Override
        public void apply(String str, TimeWindow window, Iterable<Long> input, Collector<AdCountViewByProvince> out) {
            String windowEnd = new Timestamp(window.getEnd()).toString();
            Long count = input.iterator().next();
            out.collect(new AdCountViewByProvince(str,windowEnd,count));
        }
    }
    //实现自定义处理函数
    public static class FilterBlackListUser extends KeyedProcessFunction<Tuple,AdClickEvent,AdClickEvent>{
        //点击次数上限
        private Integer countUpperBound;

        public FilterBlackListUser(Integer countUpperBound) {
            this.countUpperBound = countUpperBound;
        }
        //定义状态，保存当前用户对某一广告的点击次数
        ValueState<Long> countState;
        //标志状态保存当前用户是否已经是否进入黑名单
        ValueState<Boolean> isSentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad-count",Long.class,0L));
            isSentState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-sent",Boolean.class,false));
        }

        @Override
        public void processElement(AdClickEvent adClickEvent, Context context, Collector<AdClickEvent> collector) throws Exception {
            //判断当前用户对同一广告的点击次数
            //达到阈值输出黑名单报警
            Long curCount = countState.value();
            //判断是第一个数据，如果是的话注册一个第二天0点的定时器
            if(curCount==0){
                Long ts = (context.timerService().currentProcessingTime()/(24*60*60*1000L)+1)*(24*60*60*1000L)-8*60*60*1000L;
                context.timerService().registerProcessingTimeTimer(ts);
            }
            if(curCount >= countUpperBound){
                //判断是否输出到黑名单过，如果没有的话就输出到侧输出流
                if(!isSentState.value()){
                    isSentState.update(true);//更新状态
                    context.output(
                            new OutputTag<BlackListUserWarning>("blacklist"){},
                            new BlackListUserWarning(adClickEvent.getUserId(),adClickEvent.getAdId(),"click over "
                                    + countUpperBound+
                                    " teimes.")
                    );
                }
                return;//不执行下面操作
            }
            //如果没有返回点击次数+1更新状态
            countState.update(curCount+1);
            collector.collect(adClickEvent);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdClickEvent> out) throws Exception {
            //定时器触发清空所有状态
            countState.clear();
            isSentState.clear();
        }
    }
}
