package com.kevin.java.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author caonanqing
 * @version 1.0
 * @description     flatMap：取出一个元素并产生零个，一个或多个元素
 * @createDate 2020/3/11
 */
public class FlatMapDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> list = new ArrayList<>();
        list.add("kevin java");
        list.add("coco python");
        list.add("Mr.cao scala");

        // 将数据转成DataSet算子
        DataSource<String> data = env.fromCollection(list);

        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(s);
                }
            }
        }).print();

    }
}
