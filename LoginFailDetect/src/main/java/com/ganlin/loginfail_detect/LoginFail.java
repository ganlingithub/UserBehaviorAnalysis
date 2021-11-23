package com.ganlin.loginfail_detect;

import com.ganlin.loginfail_detect.beans.LoginEvent;
import com.ganlin.loginfail_detect.beans.LoginFailWarning;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;


public class LoginFail {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1.从文件中读取数据
        URL resource = LoginFail.class.getResource("/LoginLog.csv");
        SingleOutputStreamOperator<LoginEvent> loginEventStream = env.readTextFile(resource.getPath())
                .map(line -> {
                            String[] fields = line.split(",");
                            return new LoginEvent(
                                    new Long(fields[0]),
                                    fields[1],
                                    fields[2],
                                    new Long(fields[3])
                            );
                        }
                )
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<LoginEvent>(Time.seconds(3)) {
                            @Override
                            public long extractTimestamp(LoginEvent loginEvent) {
                                return loginEvent.getTimestamp() * 1000L;
                            }
                        }
                );
        //自定义处理函数检测连续登录失败事件
        SingleOutputStreamOperator<LoginFailWarning> warningStream = loginEventStream.keyBy(LoginEvent::getUserId)
                .process(new LoginFailDetectWarning(2));
        warningStream.print();
        env.execute("login fail detect job");
    }
    //实现自定义KeyedProcessFunction
    public static class LoginFailDetectWarning0 extends KeyedProcessFunction<Long,LoginEvent, LoginFailWarning>{
        //定义失败阈值
        private Integer maxFailTimes;

        public LoginFailDetectWarning0(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }
        //定义状态:保存2秒内登录失败次数
        ListState<LoginEvent> loginFailEventListState;
        //定义状态:保存注册的定时器时间戳
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list",LoginEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));
        }

        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<LoginFailWarning> collector) throws Exception {
            //判断当前登录事件的类型
            if("fail".equals(loginEvent.getLoginState())){
                //如果失败添加到列表中
                loginFailEventListState.add(loginEvent);
                //如果没有定时器注册一个2秒之后的定时器
                if(timerTsState.value()==null){
                    Long ts = (loginEvent.getTimestamp()+2)*1000L;
                    context.timerService().registerEventTimeTimer(ts);
                    timerTsState.update(ts);
                }
            }else{
                //如果登录成功，删除定时器，清空状态重新开始
                if(timerTsState.value()!=null){
                    context.timerService().deleteProcessingTimeTimer(timerTsState.value());
                }
                loginFailEventListState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginFailWarning> out) throws Exception {
            //定时器触发，说明2秒内没有登录成功，判断liststate中失败的个数
            ArrayList<LoginEvent> loginFailEvents = Lists.newArrayList(loginFailEventListState.get());
            Integer failTimes = loginFailEvents.size();

            if(failTimes>=maxFailTimes){
                //超过阈值触发报警
                out.collect(new LoginFailWarning(ctx.getCurrentKey(),
                        loginFailEvents.get(0).getTimestamp(),
                        loginFailEvents.get(failTimes-1).getTimestamp(),
                        "login fail in 2s for "+ failTimes + " times"
                ));
            }
            //清空状态
            loginFailEventListState.clear();
            timerTsState.clear();
        }
    }
    public static class LoginFailDetectWarning extends KeyedProcessFunction<Long,LoginEvent, LoginFailWarning>{
        //定义失败阈值
        private Integer maxFailTimes;

        public LoginFailDetectWarning(Integer maxFailTimes) {
            this.maxFailTimes = maxFailTimes;
        }
        //定义状态:保存2秒内登录失败次数
        ListState<LoginEvent> loginFailEventListState;


        @Override
        public void open(Configuration parameters) throws Exception {
            loginFailEventListState = getRuntimeContext().getListState(new ListStateDescriptor<LoginEvent>("login-fail-list",LoginEvent.class));
        }
        //以登录事件作为判断报警的触发条件，不再注册定时器
        @Override
        public void processElement(LoginEvent loginEvent, Context context, Collector<LoginFailWarning> collector) throws Exception {
            //判断当前事件登录状态
            if("fail".equals(loginEvent.getLoginState())){
                //1.如果登录失败，获取之前状态，判断是否有登录失败
                Iterator<LoginEvent> iterator = loginFailEventListState.get().iterator();
                if(iterator.hasNext()){
                    //1.1如果已经有登录失败事件，判断时间戳是否再2秒之内
                    //获取已有的登录失败事件
                    LoginEvent firstFailEvent = iterator.next();
                    if(loginEvent.getTimestamp()-firstFailEvent.getTimestamp()<=2){
                       //1.1.1如果再2秒之内，输出报警
                        collector.collect(new LoginFailWarning(loginEvent.getUserId(),firstFailEvent.getTimestamp(),loginEvent.getTimestamp(),"login fail 2 times in 2s"));
                    }
                    //不管报警是否更新状态
                    loginFailEventListState.clear();
                    loginFailEventListState.add(loginEvent);
                }else{
                    //1.2如果没有将当前时间存入ListState
                    loginFailEventListState.add(loginEvent);
                }
            }else{
                //2.登录成功,清空状态
                loginFailEventListState.clear();
            }
        }
    }
}
