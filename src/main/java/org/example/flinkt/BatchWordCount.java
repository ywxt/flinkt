package org.example.flinkt;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // create execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> lineDs = env.readTextFile("input/word.txt");


        FlatMapOperator<String, Tuple2<String, Long>> ret = lineDs.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1L));
            }
        }).returns(new TypeHint<Tuple2<String, Long>>() {
        });
        UnsortedGrouping<Tuple2<String, Long>> group = ret.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum =  group.sum(1);
        sum.print();
    }
}
