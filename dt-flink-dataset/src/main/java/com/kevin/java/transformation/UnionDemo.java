package com.kevin.java.transformation;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * @author caonanqing
 * @version 1.0
 * @description     连接数据
 * @createDate 2020/3/6
 */
public class UnionDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"java"));
        data1.add(new Tuple2<>(2,"python"));
        data1.add(new Tuple2<>(3,"scala"));

        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"kevin"));
        data2.add(new Tuple2<>(2,"coco"));
        data2.add(new Tuple2<>(3,"Mr.Cao"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);

        // text1数据连接text2数据
        UnionOperator<Tuple2<Integer, String>> union = text1.union(text2);
        union.print();

    }
}
