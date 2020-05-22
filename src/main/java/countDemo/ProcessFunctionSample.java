package countDemo;

import countDemo.CountWithTimeoutFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ProcessFunctionSample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // the source data stream
        DataStream<Tuple2<String, String>> stream = env.fromElements(Tuple2.of("nihao", "world"), Tuple2.of("hello", "world") );

        // apply the process function onto a keyed stream
        SingleOutputStreamOperator<Tuple2<String, Long>> result = stream
                .keyBy(0)
                .process(new CountWithTimeoutFunction());

        result.print();

        env.execute();
    }







}
