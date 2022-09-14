package org.example.flinkt.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        DataStreamSource<String> source = env.readTextFile("input/clicks.txt");
//
//        ArrayList<Event> events = new ArrayList<>();
//        events.add(new Event("user1", "https://www.baidu.com", 1000L));
//        events.add(new Event("user1", "https://www.baidu.com", 2000L));
//        DataStreamSource<Event> stream2 = env.fromCollection(events);
//
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "127.0.0.1:52757");
//        properties.setProperty("group.id", "consumer-group");
//        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.setProperty("auto.offset.reset", "latest");
//        DataStreamSource<String> clicks = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));
//        clicks.print();

        DataStreamSource<Event> eventSource = env.addSource(new ClickSource());
        StreamingFileSink<String> sink = StreamingFileSink.<String>forRowFormat(new Path("./output.txt"), new SimpleStringEncoder<>("UTF-8")).withRollingPolicy(DefaultRollingPolicy.builder()
                .withMaxPartSize(1024 * 1024 * 1024)
                .withInactivityInterval(TimeUnit.MINUTES.toMillis(15))
                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
                .build()
        ).build();
        eventSource.map(Event::toString)
                .addSink(sink);
        eventSource.print();

        env.execute();
    }
}
