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
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.net.URL;

public class TxPayMatch {
    //定义侧流标签
    private final static OutputTag<OrderEvent> unmatchedPays = new OutputTag<OrderEvent>("unmatched-pays"){};
    private final static OutputTag<ReceiptEvent> unmatchedReceipts = new OutputTag<ReceiptEvent>("unmatched-receipts"){};
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1.从文件中读取数据
        //读取订单支付时间数据
        URL orderResource = TxPayMatch.class.getResource("/OrderLog.csv");
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
        URL reciptResource = TxPayMatch.class.getResource("/ReceiptLog.csv");
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
        //将两条流进行连接合并，进行匹配连接,不匹配的输出到侧输出流标签
        SingleOutputStreamOperator<Tuple2<OrderEvent,ReceiptEvent>> resultStream = orderEventStream.keyBy(OrderEvent::getTxid)
                .connect(receiptEventStream.keyBy(ReceiptEvent::getTxId))
                .process(new TxPayMatchDetect());

        resultStream.print("matched-pays");
        resultStream.getSideOutput(unmatchedPays).print("unmatched-pays");
        resultStream.getSideOutput(unmatchedReceipts).print("unmatched-receipts");
        env.execute("tx match detect job");
    }
    //实现自定义CoProcessFunction
    public static class TxPayMatchDetect extends CoProcessFunction<OrderEvent,ReceiptEvent, Tuple2<OrderEvent,ReceiptEvent>>{
        //定义状态:保存已经到来的订单支付事件和到账时间
        ValueState<OrderEvent> payState;
        ValueState<ReceiptEvent> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("pay",OrderEvent.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<ReceiptEvent>("receipt",ReceiptEvent.class));
        }

        @Override
        public void processElement1(OrderEvent pay, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //订单支付事件来了，是否已经有到账事件
            ReceiptEvent receipt = receiptState.value();
            if(receipt!=null){
                //如果receipt不为空说明到账事件已经来过，输出匹配事件清空状态
                out.collect(new Tuple2<>(pay,receipt));
                payState.clear();
                receiptState.clear();
            }else{
                //如果receipt没来注册定时器等待,这里取5s
                ctx.timerService().registerEventTimeTimer((pay.getTimestamp()+5)*1000L);
                //更新状态
                payState.update(pay);
            }
        }

        @Override
        public void processElement2(ReceiptEvent receipt, Context ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //到账事件来了，是否已经有到账事件
           OrderEvent pay = payState.value();
            if(receipt!=null){
                //如果receipt不为空说明到账事件已经来过，输出匹配事件清空状态
                out.collect(new Tuple2<>(pay,receipt));
                payState.clear();
                receiptState.clear();
            }else{
                //如果pay没来注册定时器等待,这里取3s
                ctx.timerService().registerEventTimeTimer((receipt.getTimestamp()+3)*1000L);
                //更新状态
                receiptState.update(receipt);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            //定时器触发，有可能有一个事件没来不匹配，也有可能是都来过了，已经输出并清空状态
            //判断哪个不为空所以另外一个就没来
            //这样做可以省去删除定时器步骤，少用一个定时器时间戳状态
            if(payState.value()!=null){
                ctx.output(unmatchedPays,payState.value());
            }
            if(receiptState.value()!=null){
                ctx.output(unmatchedReceipts,receiptState.value());
            }
            //清空状态
            payState.clear();
            receiptState.clear();
        }
    }
}
