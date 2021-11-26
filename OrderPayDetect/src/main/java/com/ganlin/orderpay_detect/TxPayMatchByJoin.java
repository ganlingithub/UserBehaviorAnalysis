package com.ganlin.orderpay_detect;

import com.ganlin.orderpay_detect.beans.OrderEvent;
import com.ganlin.orderpay_detect.beans.ReceiptEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class TxPayMatchByJoin {
    //定义侧流标签
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays"){};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts"){};
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1.从文件中读取数据
        //读取订单支付时间数据
        URL orderResource = TxPayMatchByJoin.class.getResource("/OrderLog.csv");
        SingleOutputStreamOperator<OrderEvent> orderEventStream = env.readTextFile(orderResource.getPath())
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
                )
                .filter(//交易ID不为空，必须是支付事件
                        data -> !"".equals(data.getTxid())
                );
        //读取到账时间数据
        URL reciptResource = TxPayMatchByJoin.class.getResource("/ReceiptLog.csv");
        SingleOutputStreamOperator<ReceiptEvent> receiptEventStream = env.readTextFile(reciptResource.getPath())
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ReceiptEvent(
                            fields[0],
                            fields[1],
                            new Long(fields[2])
                    );
                })
                .assignTimestampsAndWatermarks(
                        new AscendingTimestampExtractor<ReceiptEvent>() {
                            @Override
                            public long extractAscendingTimestamp(ReceiptEvent receiptEvent) {
                                return receiptEvent.getTimestamp() * 1000L;
                            }
                        }
                );
        //区间连接两条流，得到匹配的数据
        SingleOutputStreamOperator<Tuple2<OrderEvent,ReceiptEvent>> resultStream = orderEventStream
                .keyBy(OrderEvent::getTxid)
                .intervalJoin(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .between(Time.seconds(-3), Time.seconds(5))//-3到5的区间范围
                .process(new TxPayMatchDetectByJoin());
        //只能得到匹配到的结果
        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");
        env.execute("tx pay match by join job");
    }
    //实现自定义ProcessJoinFunction
    public static class TxPayMatchDetectByJoin extends ProcessJoinFunction<OrderEvent,ReceiptEvent,Tuple2<OrderEvent,ReceiptEvent>>{
        @Override
        public void processElement(OrderEvent left, ReceiptEvent right, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            out.collect(new Tuple2<>(left,right));
        }
    }
}
