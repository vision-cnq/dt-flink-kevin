package com.kevin.java;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * @author caonanqing
 * @version 1.0
 * @description     获取集合中的前N个元素
 * @createDate 2020/3/6
 */
public class FirstDemo {

    public static void main(String[] args) throws Exception {

        // 1.获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<Integer,String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2,"java"));
        data.add(new Tuple2<>(1,"python"));
        data.add(new Tuple2<>(3,"scala"));
        data.add(new Tuple2<>(4,"java"));
        data.add(new Tuple2<>(5,"python"));
        data.add(new Tuple2<>(1,"scala"));
        data.add(new Tuple2<>(2,"python"));
        data.add(new Tuple2<>(1,"scala"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);
        // 获取前3条数据，按照数据插入的顺序
        text.first(3).print();
        System.out.println("-----------------------");

        // 根据数据的第一列进行分组，获取每组的前2个元素
        text.groupBy(0).first(2).print();
        System.out.println("-----------------------");

        // 根据数据中的第一列分组，再根据第二列进行组内升序排序，获取前2条数据
        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("-----------------------");

        // 不分组，全局排序获取集合中的前3条,第一个元素升序，第二个元素倒序
        text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print();

    }

}
