package com.kevin.java;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;

/**
 * @author caonanqing
 * @version 1.0
 * @description     全局累加器
 * @createDate 2020/3/10
 */
public class CounterDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {

            // 1.创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 2.注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String value) throws Exception {
                // 如果并行度为1,使用普遍的累加器求和即可,但是设置多个并行度，则普通的累加求和结果就不准了
                this.numLines.add(1);
                return value;
            }
        }).setParallelism(8);   // 设置并行度为8

        result.writeAsText("e:\\data\\count");
        JobExecutionResult jobResult = env.execute("counter");

        // 3.获取累加器
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num:"+num);

    }
}
