package com.kevin.java.sink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author caonanqing
 * @version 1.0
 * @description
 * @createDate 2020/4/12
 */
public class CsvDemo {

    public static void main(String[] args) throws Exception {
        String inputPath = "DTFlinkBatch/src/main/resources/words.txt";
        String outPath = "D:/big_data/result.csv";

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 获取文件中的内容
        DataSource<String> text = env.readTextFile(inputPath);
        // 切分数据，初始化单词数值为1，按照第一位元素进行分组，第二位元素进行相加
        AggregateOperator<Tuple2<String, Integer>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String value : s.split(" ")) {
                    out.collect(new Tuple2<String, Integer>(value, 1));
                }
            }
        }).groupBy(0).sum(1);

        counts.writeAsCsv(outPath,"\n"," ").setParallelism(1);
        env.execute("csv demo");

    }
}
