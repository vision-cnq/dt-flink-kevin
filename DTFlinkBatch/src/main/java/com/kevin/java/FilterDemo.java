package com.kevin.java;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @author caonanqing
 * @version 1.0
 * @description     filter
 *      过滤符合条件的数据，true保留，false过滤掉
 * @createDate 2020/3/12
 */
public class FilterDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取本地文件
        DataSource<String> data = env.readTextFile("DTFlinkBatch\\src\\main\\resources\\test.txt");

        data.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                // 过来不包含server的单词
                return !s.contains("server");
            }
        }).print();

    }
}
