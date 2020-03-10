package com.kevin.java;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author caonanqing
 * @version 1.0
 * @description     DistributedCache
 *      分布式缓存：可以使用户在并行函数中很方便的读取本地文件，并把它放在taskmanager节点中，防止task重复拉取。
 * @createDate 2020/3/9
 */
public class DistributedCacheDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 程序注册一个文件或者目录(本地或者远程文件系统，例如hdfs或者s3)
        // 注册文件保存到taskmanager节点的本地文件系统，仅会执行一次，之后可以通过所指定的名字访问它
        env.registerCachedFile("e:\\data\\file\\a.txt","a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 读取已注册的文件
                File file = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> list = FileUtils.readLines(file);
                for (String line : list) {
                    this.dataList.add(line);
                    System.out.println("line: " + line);
                }
            }
            @Override
            public String map(String value) throws Exception {
                // 这里可以使用dataList
                return value;
            }

        });

        result.print();

    }
}
