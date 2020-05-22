package randomSample;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Char;

import java.util.LinkedList;
import java.util.List;


public class RandomSample {

    public static void main(String[] args) throws Exception {

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // generate dataSource
        String string1 = RandomStringUtils.randomAlphabetic(100);
        List<Character> list1 = new LinkedList<>();
        for (int i = 0; i < string1.length(); i++) {
            list1.add(string1.charAt(i));
        }

        String string2 = RandomStringUtils.randomAlphabetic(100);
        List<Character> list2 = new LinkedList<>();
        for (int i = 0; i < string2.length(); i++) {
            list2.add(string2.charAt(i));
        }

        DataStreamSource<Character> characterDataStreamSource1 = env.fromCollection(list1);
        DataStreamSource<Character> characterDataStreamSource2 = env.fromCollection(list2);

        // apply updateFunction and mergeFunction on datasource
        DataStream<SampleRecord> sampleRecordStream1 = characterDataStreamSource1
                .map(new MapFunction<Character, Tuple2<Integer, Character>>() {
                    @Override
                    public Tuple2<Integer, Character> map(Character character) throws Exception {
                        return new Tuple2<>(1, character);
                    }
                })
                .keyBy(0)
                .process(new UpdateFunction());
        DataStream<SampleRecord> sampleRecordStream2 =
                characterDataStreamSource2
                        .map(new MapFunction<Character, Tuple2<Integer, Character>>() {
                            @Override
                            public Tuple2<Integer, Character> map(Character character) throws Exception {
                                return new Tuple2<>(1, character);
                            }
                        })
                        .keyBy(0)
                        .process(new UpdateFunction());
        sampleRecordStream1.print("sampleRecordStream1: ");
        sampleRecordStream2.print("sampleReocrdStream2: ");

        DataStream<SampleRecord> mergedStream =
                sampleRecordStream1
                        .map(new MapFunction<SampleRecord, Tuple2<Integer, SampleRecord>>() {
                            public Tuple2<Integer, SampleRecord> map(SampleRecord sampleRecord) throws Exception {
                                return new Tuple2<>(1, sampleRecord);
                            }
                        })
                        .keyBy(0)
                        .connect(sampleRecordStream2
                                .map(new MapFunction<SampleRecord, Tuple2<Integer, SampleRecord>>() {
                                    public Tuple2<Integer, SampleRecord> map(SampleRecord sampleRecord) throws Exception {
                                        return new Tuple2<>(1, sampleRecord);
                                    }
                                })
                                .keyBy(0))
                        .flatMap(new MergeFunction());

        mergedStream.print("mergedStream: ");

        env.execute();

    }
}