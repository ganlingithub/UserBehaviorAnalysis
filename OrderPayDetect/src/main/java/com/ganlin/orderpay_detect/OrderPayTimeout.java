package com.ganlin.orderpay_detect;

import com.ganlin.orderpay_detect.beans.OrderEvent;
import com.ganlin.orderpay_detect.beans.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class OrderPayTimeout {
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
        //1.定义一个带时间限制的模式
        Pattern<OrderEvent, OrderEvent> orderPayPattrtn = Pattern
                .<OrderEvent>begin("create").where(
                        new SimpleCondition<OrderEvent>() {
                            @Override
                            public boolean filter(OrderEvent orderEvent) throws Exception {
                                return "create".equals(orderEvent.getEventType());
                            }
                        }
                )
                .followedBy("pay").where(
                        new SimpleCondition<OrderEvent>() {
                            @Override
                            public boolean filter(OrderEvent orderEvent) throws Exception {
                                return "pay".equals(orderEvent.getEventType());
                            }
                        }
                )
                .within(Time.minutes(15));
        //2.定义侧输出流标签,用来表示超时事件
        OutputTag<OrderResult> orderTimeoutTag = new OutputTag<OrderResult>("order-timeout"){};
        //3.将pattern应用到输入数据流上，得到pattern stream
        PatternStream<OrderEvent> patternStream = CEP.pattern(orderEventStream.keyBy(OrderEvent::getOrderId), orderPayPattrtn);
        //4.调用select方法,实现对匹配复杂事件和超时复杂事件的提取和处理
        SingleOutputStreamOperator<OrderResult> resultStream = patternStream.select(orderTimeoutTag, new OrderTimeoutSelect(), new OrderPaySelect());

        resultStream.print("payed normally");
        resultStream.getSideOutput(orderTimeoutTag).print("order timeout detect job");
        env.execute();
    }
    //实现自定义超时事件处理函数
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderEvent,OrderResult>{
        @Override
        public OrderResult timeout(Map<String, List<OrderEvent>> map, long timeoutTimeStamp) throws Exception {
            Long timeoutOrderId = map.get("create").iterator().next().getOrderId();
            return new OrderResult(timeoutOrderId,"timeout"+timeoutTimeStamp);
        }
    }
    //实现自定义的正常匹配事件的处理函数
    public static class OrderPaySelect implements PatternSelectFunction<OrderEvent,OrderResult>{
        @Override
        public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
            Long payedOrderId = map.get("create").iterator().next().getOrderId();
            return new OrderResult(payedOrderId,"payed");
        }
    }
}
