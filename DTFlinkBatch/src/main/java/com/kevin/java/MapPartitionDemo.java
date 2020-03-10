package com.kevin.java;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author caonanqing
 * @version 1.0
 * @description     mapPartition案例，主要用在数据库连接
 * @createDate 2020/3/9
 */
public class MapPartitionDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<String> list = new ArrayList<>();
        list.add("kevin java");
        list.add("coco python");
        list.add("Mr.cao scala");

        DataSource<String> text = env.fromCollection(list);

        // 每次来一个分区的数据进行处理
        DataSet<String> mapPartition = text.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> values, Collector<String> out) throws Exception {
                // 1.获取数据库连接,每个分区获取一个连接
                // 2.处理数据,values保存一个分区的数据
                Iterator<String> it = values.iterator();
                while(it.hasNext()) {
                    String next = it.next();
                    String[] str = next.split(" ");
                    for (String value : str) {
                        out.collect(value);
                    }
                }
                // 3.关闭连接
            }
        });

        mapPartition.print();

    }
}
