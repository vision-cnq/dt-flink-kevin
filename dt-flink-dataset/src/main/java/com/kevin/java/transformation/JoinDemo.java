package com.kevin.java.transformation;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * @author caonanqing
 * @version 1.0
 * @description     批处理：内连接
 * @createDate 2020/3/9
 */
public class JoinDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1,"kevin"));
        data1.add(new Tuple2<>(2,"coco"));
        data1.add(new Tuple2<>(3,"Mr.Cao"));

        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1,"shenzhen"));
        data2.add(new Tuple2<>(2,"guangzhou"));
        data2.add(new Tuple2<>(5,"zhanjiang"));

        DataSource<Tuple2<Integer, String>> text1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> text2 = env.fromCollection(data2);


        text1.join(text2).where(0)  // 第一个数据集中需要进行比较的元素
                .equalTo(0) // 第二个数据集汇总需要进行比较的元素
                .with(
                new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<>(first.f0,first.f1,second.f1);
            }
        }).print();

        System.out.println("------------------");

        // 这里使用map和上面with的效果一致
        text1.join(text2).where(0).equalTo(0).map(
                new MapFunction<Tuple2<Tuple2<Integer,String>,Tuple2<Integer,String>>, Tuple3<Integer,String,String>>() {
            @Override
            public Tuple3<Integer, String, String> map(Tuple2<Tuple2<Integer, String>, Tuple2<Integer, String>> value) throws Exception {
                return new Tuple3<>(value.f0.f0,value.f0.f1,value.f1.f1);
            }
        }).print();


    }

}
