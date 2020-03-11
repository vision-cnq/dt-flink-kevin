package com.kevin.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;

/**
 * @author caonanqing
 * @version 1.0
 * @description     map：每来一个元素则处理一个元素
 * @createDate 2020/3/9
 */
public class MapDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> list = new ArrayList<>();
        list.add("kevin java");
        list.add("coco python");
        list.add("Mr.cao scala");

        DataSource<String> text = env.fromCollection(list);

        // 每来一条数据处理一条数据
        DataSet<String> map = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                // 1.获取数据库连接,每条数据获取一个连接
                // 2.处理数据
                // 3.关闭连接
                return value;
            }
        });

        // 输出数据
        map.print();

    }
}
