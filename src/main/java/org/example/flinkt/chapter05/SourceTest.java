package org.example.flinkt.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

//        DataStreamSource<String> source = env.readTextFile("input/clicks.txt");
//
//        ArrayList<Event> events = new ArrayList<>();
//        events.add(new Event("user1", "https://www.baidu.com", 1000L));
//        events.add(new Event("user1", "https://www.baidu.com", 2000L));
//        DataStreamSource<Event> stream2 = env.fromCollection(events);
//
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "lab3:9092,lab4:9092");
        properties.setProperty("group.id", "consumer-bigdata");
//        properties.setProperty("max.poll.records", "500");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("bigdata1", new SimpleStringSchema(), properties);

        Properties pProperties = new Properties();
        pProperties.setProperty("bootstrap.servers", "lab3:9092,lab4:9092");
//        pProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        pProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("bigdata1-result", new SimpleStringSchema(), pProperties);

//        Properties pProperties = new Properties();
//        pProperties.setProperty("bootstrap.servers", "lab3:9092");
//        pProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        pProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("clicks-test", new SimpleStringSchema(), pProperties);
////        clicks.print();

        DataStreamSource<String> eventSource = env.addSource(consumer).setParallelism(4);
//        StreamingFileSink<String> sink = StreamingFileSink.<String>forRowFormat(new Path("./output.txt"), new SimpleStringEncoder<>("UTF-8")).withRollingPolicy(DefaultRollingPolicy.builder()
//                .withMaxPartSize(1024 * 1024 * 1024)
//                .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
//                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
//                .build()
//        ).build();
        SingleOutputStreamOperator<Tuple2<String, Long>> stream = eventSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        }).setParallelism(8);
//        SingleOutputStreamOperator<String> filter = stream.filter(new FilterFunction<Tuple2<String, Long>>() {
//            @Override
//            public boolean filter(Tuple2<String, Long> stringLongTuple2) throws Exception {
//                return stringLongTuple2.f1 > 1000;
//            }
//        }).setParallelism(4).map(group -> group.f0 + "\t" + group.f1).setParallelism(4);
//        stream.addSink(producer).name("Producer");
        SingleOutputStreamOperator<String> sum = stream.keyBy(word -> word.f0).sum(1).setParallelism(8).map(group -> group.f0 + "\t" + group.f1)
                .setParallelism(4);
//        SingleOutputStreamOperator<String> filter = sum.filter(x -> x.hashCode() % 10 == 3 || x.hashCode() % 10 == 6 || x.hashCode() % 10 == 9).setParallelism(8);
        sum.addSink(producer).setParallelism(8);
        env.execute();
    }
}
