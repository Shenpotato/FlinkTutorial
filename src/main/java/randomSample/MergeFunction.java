package randomSample;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class MergeFunction extends RichCoFlatMapFunction<Tuple2<Integer, SampleRecord>, Tuple2<Integer, SampleRecord>, SampleRecord> {

    private ValueState<SampleRecord> state1;
    private ValueState<SampleRecord> state2;

    @Override
    public void open(Configuration parameters) throws Exception {
        state1 = getRuntimeContext().getState(new ValueStateDescriptor<SampleRecord>("sampleState1", SampleRecord.class));
        state2 = getRuntimeContext().getState(new ValueStateDescriptor<SampleRecord>("sampleState2", SampleRecord.class));
    }

    @Override
    public void flatMap1(Tuple2<Integer, SampleRecord> value, Collector<SampleRecord> out) throws Exception {
        SampleRecord sampleRecord2 = state2.value();
        if (sampleRecord2 != null) {
            SampleRecord result = value.f1.merge(sampleRecord2);
            state2.clear();
            out.collect(result);
        } else {
            state1.update(value.f1);
        }
    }

    @Override
    public void flatMap2(Tuple2<Integer, SampleRecord> value, Collector<SampleRecord> out) throws Exception {
        SampleRecord sampleRecord1 = state1.value();
        if (sampleRecord1 != null) {
            SampleRecord record = value.f1.merge(sampleRecord1);
            state1.clear();
            out.collect(record);
        } else {
            state2.update(value.f1);
        }
    }
}
