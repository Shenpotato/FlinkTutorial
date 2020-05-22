import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Map;


public class StreamDemo {
    private static final Joiner ADD_JOINER = Joiner.on(":");

    public static void main(String[] args) throws Exception {
        //1. 构建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. 定义数据源（source），这里使用监听9000端口的socket消息，通常也可用来测试flink任务
        //nc -lk 9000 开启本地端口
        DataStream<String> socketTextStream = env.socketTextStream("localhost", 9000, "\n");

        /*
        3.1 map DataStream -> DataStream，流属性不变
                类似于传统的map，一个key对应一个value，map算子一个输入对应一个输出
        * */
//        DataStream<String> processSocketTextStream = socketTextStream.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String s) throws Exception {
//                return ADD_JOINER.join("socker message", s);
//            }
//        });
        //使用lambda表达式
//        DataStream<String> processSocketTextStreamLambda = socketTextStream
//                .map((MapFunction<String, String>) value -> ADD_JOINER.join("socket message", value));

        /*
        3.2 flatMap DataStream -> DataStream，流属性不变
                    相当于平铺，输入一个元素，输出0，1或多个元素。以下实现以空格风哥字符串，宁输出分割后的字符串集合
        * */
        DataStream<String> processSocketTextStream = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                for(String v : s.split(" ")){
                    collector.collect(v);
                }
            }
        });

        /*
        3.3 filter DataStream -> DataStream，流属性不变
                   过滤器，对数据流中的每个元素进行过滤判断，判断为true的元素进入以下一个数据流
        * */
//        DataStream<String> processSocketTextStream = socketTextStream.filter(new FilterFunction<String>() {
//            @Override
//            public boolean filter(String s) throws Exception {
//                return s.startsWith("hello");
//            }
//        });

        /*
        3.4 keyBy DataStream -> KeyedStream
                  将数据流按照key分成多个不相交的分区，相同的key的会被分到同一个分区中，keyBy()通过散列分区实现
                  可以将一个类的一个或多个属性当作key，也可以将tuple的元素当作key，但是有两种类型的不能作为key
                  - 1. 没有重写hashCode方法
                  - 2. 数组类型
        3.5 Reduce KeyedStream -> DataStream
                   KeyeStream经过ReduceFunction后变成DataStream，对每个分区中的元素进行规约操作，每个分区只输出一个值
        * */
//        DataStream<String> studentdata = env.readTextFile("src/main/resources/student.txt");
//        DataStream<Student> flatMapSocketTextStream = studentdata.flatMap(new FlatMapFunction<String, Student>() {
//            @Override
//            public void flatMap(String s, Collector<Student> collector) throws Exception {
//                String[] values = s.split(" ");
//                collector.collect(new Student(values[0], values[1], values[2], Integer.valueOf(values[3])));
//            }
//        });

        //当attribute作为key时，需要有一个无参构造器
//        DataStream<Student> processSocketTextStream = flatMapSocketTextStream
//                .keyBy("gender")
//                .reduce((ReduceFunction<Student>) (s1, s2) -> s1.getScore() > s2.getScore() ? s1 : s2);

        /*
        3.6 Fold KeyedStream -> DataStream
                 Fold 与 reduce的区别在于Fold可以设置初始值
        * */

        /*
        3.7 Aggregations KeyedSteam -> DataStream
                         包含min，max，sum等算子
        * */

        /*
        3.8 union & join
            union 将多个DataFrame拼接在一起，join将两个DataFrame join在一起
        * */

        //4. 打印
        processSocketTextStream.print();

        //5. 执行
        env.execute();

    }


}
