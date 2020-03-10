package com.kevin.java;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;

/**
 * @author caonanqing
 * @version 1.0
 * @description     获取笛卡尔积
 *      创建所有数据元对，可以选择使用CrossFunction将数据元对转换为单个数据元。
 * @createDate 2020/3/10
 */
public class CrossDemo {

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

        CrossOperator.DefaultCross<Tuple2<Integer, String>, Tuple2<Integer, String>> cross = text1.cross(text2);
        cross.print();

    }
}
