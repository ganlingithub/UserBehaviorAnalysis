package com.ganlin.orderpay_detect;

import com.ganlin.orderpay_detect.beans.OrderEvent;
import com.ganlin.orderpay_detect.beans.OrderResult;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class OrderTimeoutWithoutCep {
    //定义超时事件的侧输出流标签
    private final static OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1.从文件中读取数据
        URL resource = OrderPayTimeout.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                            String[] fields = line.split(",");
                            return new OrderEvent(
                                    new Long(fields[0]),
                                    fields[1],
                                    fields[2],
                                    new Long(fields[3])
                            );
                        }
                )
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<OrderEvent>() {
                            @Override
                            public long extractAscendingTimestamp(OrderEvent orderEvent) {
                                return orderEvent.getTimestamp()*1000L;
                            }
                        }
                );
        //1.自定义处理函数，主流输出成功支付事件，侧流输出超时报警事件
        SingleOutputStreamOperator<OrderResult> resultStream = orderEventStream.keyBy(OrderEvent::getOrderId)
                .process(new OrderPayMatchDetect());
        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("timeout");
        env.execute("order timeout detect without cep job");
    }
    //实现自定义的keyed的processFunction
    public static class OrderPayMatchDetect extends KeyedProcessFunction<Long,OrderEvent,OrderResult>{
        //定义状态，之前订单是否已经来过create、pay的事件
        ValueState<Boolean> isPayedState;
        ValueState<Boolean> isCreatedState;
        //保存定时器时间戳
        ValueState<Long> timerTsState;
        @Override
        public void open(Configuration parameters) throws Exception {
            isPayedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-payed",Boolean.class,false));
            isCreatedState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is-created",Boolean.class,false));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-Ts",Long.class));
        }

        @Override
        public void processElement(OrderEvent orderEvent, Context context, Collector<OrderResult> collector) throws Exception {
            //先获取当前状态
            Boolean isPayed = isPayedState.value();
            Boolean isCreated = isCreatedState.value();
            Long timerTs = timerTsState.value();

            //判断当前的事件类型
            if("create".equals(orderEvent.getEventType())){
                //1.如果是create类型，要判断是否支付过
                if(isPayed){
                    //1.1如果已经正常支付，输出正常匹配结果
                    collector.collect(new OrderResult(orderEvent.getOrderId(),"payed successfully"));
                    //清空状态，删除i当时器
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    context.timerService().deleteProcessingTimeTimer(timerTs);
                }else{
                    //1.2如果没有支付过，注册15分钟后的定时器，开始等待支付事件
                    Long ts =(orderEvent.getTimestamp()+15*60)*1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    //更新状态
                    timerTsState.update(ts);
                    isCreatedState.update(true);

                }
            }else if("pay".equals(orderEvent.getEventType())){
                //2.如果来的是pay，要判断是否有下单事件来过
                if(isCreated){
                    //2.1有过下单事件，要继续判断支付时间戳是否超过15分钟
                    if(orderEvent.getTimestamp()*1000L<timerTs){
                        //2.1.1 在15分钟内，没有超时，正常匹配输出
                        collector.collect(new OrderResult(orderEvent.getOrderId(),"payed successfully"));
                    }else{
                        //2.1.2 已经超时，输出侧输出流
                        context.output(orderTimeoutTag,new OrderResult(orderEvent.getOrderId(),"payed but already timeout"));
                    }
                    //清空状态，删除i当时器
                    isCreatedState.clear();
                    isPayedState.clear();
                    timerTsState.clear();
                    context.timerService().deleteProcessingTimeTimer(timerTs);
                }
            }else{
                //2.2没有下单事件，乱序，注册一个定时器，等待下单事件
                //利用watermark延迟直接注册一个当前时间戳的定时器
                context.timerService().registerEventTimeTimer(orderEvent.getTimestamp()*1000L);
                //更新状态
                timerTsState.update(orderEvent.getTimestamp()*1000L);
                isPayedState.update(true);

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            //定时器触发，说明一定有一个时间没来
            if(isPayedState.value()){
                //如果pay来了，说明create每来
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(),"payed but not found created log"));
            }else{
                //pay没来支付超时
                ctx.output(orderTimeoutTag, new OrderResult(ctx.getCurrentKey(),"timeout"));
            }
            //状态清理，定时器已触发故不用删除
            isCreatedState.clear();
            isPayedState.clear();
            timerTsState.clear();
        }
    }
}
