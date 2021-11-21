package com.ganlin.networkflow_analysis;

import com.ganlin.networkflow_analysis.beans.PageViewCount;
import com.ganlin.networkflow_analysis.beans.UserBehavior;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.net.URL;
import java.util.HashSet;

public class UvWithBloomFilter {
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
                .trigger(new MyTrigger())
                .process(new UvCountResultWithBloomFilter());
        uvStream.print();
        env.execute("uv count with bloom filter job");
    }
    //自定义触发器
    public static class MyTrigger extends Trigger<UserBehavior,TimeWindow>{
        @Override
        public TriggerResult onElement(UserBehavior userBehavior, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            //每一条数据来了之后就触发窗口计算并清空窗口
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
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
    //自定义一个布隆过滤器
    public static class MyBloomFilter{
        //定义位图大小,一般定义为2的整次幂
        private Integer cap;

        public MyBloomFilter(Integer cap) {
            this.cap = cap;
        }
        //实现一个hash函数
        public long hashCode(String val,Integer seed){
            long result = 0L;
            for(int i=0;i<val.length();i++){
                result = result*seed+val.charAt(i);
            }
            return result & (cap - 1);
        }
    }
    //实现自定义的处理函数
    public static class UvCountResultWithBloomFilter extends ProcessAllWindowFunction<UserBehavior,PageViewCount,TimeWindow> {
        //定义jedis连接和布隆过滤器
        Jedis jedis;
        MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost",6379);
            //处理一亿个数据，用64MB大小的位图
            myBloomFilter = new MyBloomFilter(1<<29);
        }

        @Override
        public void process(Context context, Iterable<UserBehavior> iterable, Collector<PageViewCount> collector) throws Exception {
            //将位图和窗口的count值全部存入redis，用windowend作为key
            Long windowEnd = context.window().getEnd();
            String bitmapKey = windowEnd.toString();
            //count值存为hash表
            String countHashName = "uv_count";
            String countKey = windowEnd.toString();

            //1.取当前的userId
            Long userId = iterable.iterator().next().getUserId();

            //2.计算位图中的偏移量
            Long offset = myBloomFilter.hashCode(userId.toString(),61);

            //3.判断是否存在，redis gitbit命令,判断对应位置的值
            Boolean isExist = jedis.getbit(bitmapKey,offset);

            if(!isExist){
                //如果不存在对应位图位置置成1
                jedis.setbit(bitmapKey,offset,true);
                //更新redis中的count值
                Long uvCount = 0L;
                String uvCountString = jedis.hget(countHashName, countKey);
                if(uvCountString != null && !"".equals(uvCountString)){
                    uvCount = Long.valueOf(uvCountString);
                }
                jedis.hset(countHashName,countKey,String.valueOf(uvCount+1));

                collector.collect(new PageViewCount("uv",windowEnd,uvCount+1));
            }
        }

        @Override
        public void close() throws Exception {
            jedis.close();
        }
    }
}
