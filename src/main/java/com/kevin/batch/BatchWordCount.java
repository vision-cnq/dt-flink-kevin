package com.kevin.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author caonanqing
 * @version 1.0
 * @description
 * @createDate 2020/2/27
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {

        // 1.获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2.创建数据集
        DataSet<String> text = env.fromElements("java java scala", "scala java python");
        // 3.flatMap将数据转成大写并以空格进行分割
        // groupBy归纳相同的key，sum将value相加
        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] value = s.toLowerCase().split(" ");
                for (String word : value) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        })
        .groupBy(0)
        .sum(1);

        // 4.打印
        counts.print();

    }
}
