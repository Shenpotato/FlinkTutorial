import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class RandomSample {
    // the source data stream
    DataStream<Tuple2<String, String>> stream;

    // apply the process function onto a keyed stream
    SingleOutputStreamOperator<Tuple2<String, Long>> result = stream
            .keyBy(0)
            .process(new CountWithTimeoutFunction());

    public class CountWithTImestamp{
        public String key;
        public long count;
        public long lastModified;
    }

    public class CountWithTimeoutFunction extends ProcessFunction<Tuple2<String, String>, Tuple2<String, Long>> {

        public void processElement(Tuple2<String, String> stringStringTuple2, Context context, Collector<Tuple2<String, Long>> collector) throws Exception {

        }
    }



}
