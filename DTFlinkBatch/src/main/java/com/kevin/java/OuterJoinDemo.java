package com.kevin.java;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * @author caonanqing
 * @version 1.0
 * @description     外连接，左外连接，右外连接，全外连接
 * @createDate 2020/3/6
 */
public class OuterJoinDemo {

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

        // 左外连接
        text1.leftOuterJoin(text2).where(0).equalTo(0).with(
                new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>()  {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                // 左边全部，右边不存在则为null
                if(second == null) {
                    return new Tuple3<>(first.f0,first.f1,"null");
                } else {
                    return new Tuple3<>(first.f0,first.f1,second.f1);
                }
            }
        }).print();

        System.out.println("------------------");


    }
}
