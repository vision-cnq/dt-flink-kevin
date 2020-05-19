package com.kevin.java.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author caonanqing
 * @version 1.0
 * @description     数据去重
 * @createDate 2020/3/6
 */
public class DistinctDemo {

    public static void main(String[] args) throws Exception {

        // 1.获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("java python");
        data.add("java scala");
        data.add("python scala");
        data.add("scala java");

        DataSource<String> text = env.fromCollection(data);
        FlatMapOperator<String, String> flatMap = text.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] str = value.toLowerCase().split(" ");
                for (String word : str) {
                    System.out.println("value: " + word);
                    out.collect(word);
                }
            }
        });

        // 对数据进行去重
        flatMap.distinct().print();

    }
}
