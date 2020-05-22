package countDemo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

    private ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<CountWithTimestamp>("myState", CountWithTimestamp.class));
    }

    @Override
    public void processElement(Tuple2<String, String> stringStringTuple2, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {

        // 查看当前计数
        CountWithTimestamp current = state.value();
        if(current == null){
            current = new CountWithTimestamp();
            current.key = stringStringTuple2.f0;
        }

        // 更新状态中的计数
        current.count++;

        // 设置状态的时间戳为记录事件时间戳
        current.lastModified = context.timestamp();

        // 状态回写
        state.update(current);

        // 从当前事件时间开始注册一个60s的定时器
        context.timerService().registerEventTimeTimer(current.lastModified + 60000);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {

        // 得到设置这个定时器的键对应状态
        CountWithTimestamp result = state.value();

        // 检查定时器是过时定时器还是最新定时器
        if(timestamp == result.lastModified + 60000){
            // emit the state on timeout
            out.collect(new Tuple2<String, Long>(result.key, result.count));
        }
    }
}
