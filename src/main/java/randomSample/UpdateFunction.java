package randomSample;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.temporal.ChronoField;
import java.util.Random;

/**
 * @author ShenPotato
 * @descrpiton implements update summary operation by extends ProcessFunction
 * @date 2020/5/18 10:01 上午
 */

public class UpdateFunction extends ProcessFunction<Tuple2<Integer, Character>, SampleRecord> {

    private ValueState<SampleRecord> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        state = getRuntimeContext().getState(new ValueStateDescriptor<SampleRecord>("updateState", SampleRecord.class));
    }


    @Override
    public void processElement(Tuple2<Integer, Character> value, Context ctx, Collector<SampleRecord> out) throws Exception {
        SampleRecord current = state.value();
        if (current == null) {
            current = new SampleRecord(10);
        }

        current.update(value.f1);

        // rewrite state
        state.update(current);
//        System.out.println(current);
        out.collect(current);
    }
}
