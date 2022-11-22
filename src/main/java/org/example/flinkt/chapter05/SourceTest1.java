package org.example.flinkt.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class SourceTest1 {
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
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "lab3:9092,lab4:9092");
//        properties.setProperty("group.id", "consumer-test");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("clicks-test", new SimpleStringSchema(), properties);

        Properties pProperties = new Properties();
        pProperties.setProperty("bootstrap.servers", "lab3:9092,lab4:9092");
//        pProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        pProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("clicks-test1", new SimpleStringSchema(), pProperties);
////        clicks.print();

        DataStreamSource<Event> eventSource = env.addSource(new ClickSource());
//        StreamingFileSink<String> sink = StreamingFileSink.<String>forRowFormat(new Path("./output.txt"), new SimpleStringEncoder<>("UTF-8")).withRollingPolicy(DefaultRollingPolicy.builder()
//                .withMaxPartSize(1024 * 1024 * 1024)
//                .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
//                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
//                .build()
//        ).build();
        SingleOutputStreamOperator<String> stream = eventSource.map(Event::toString);
//        stream.addSink(producer).name("Producer");
        stream.addSink(producer);

        env.execute();
    }
}
