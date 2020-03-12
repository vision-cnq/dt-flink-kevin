package com.kevin.java;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @author caonanqing
 * @version 1.0
 * @description     reduce
 *      将两个元素重复组合成一个元素，将一组元素组合成一个元素
 * @createDate 2020/3/12
 */
public class ReduceDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 创建数据集
        DataSource<Integer> data = env.fromElements(13, 214, 23, 54, 5435, 654, 76, 567, 65, 7, 657, 65, 765, 8, 2, 4, 124);
        // 聚合，将数据全部求和
        data.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer a, Integer b) throws Exception {
                return a+b;
            }
        }).print();

    }
}
