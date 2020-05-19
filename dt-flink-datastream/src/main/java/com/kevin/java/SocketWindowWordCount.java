package com.kevin.java;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author caonanqing
 * @version 1.0
 * @description     窗口函数
 *      实现每隔1秒对最近2秒内的数据进行汇总计算
 * @createDate 2020/3/26
 */
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        /**
         *  开发步骤
         *    1.获取运行环境
         *    2.加载/创建/初始化数据
         *    3.指定操作数据的transaction算子
         *    4.指定把计算好的数据存放
         *    5.调用execute()触发程序
         *
         *   flink为延迟计算，需要最后调用execute之后才会运行程序。
         *
         */
        
        // 获取需要的端口号
        int port;
        // 获取需要的主机名
        String hostname;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
            hostname = parameterTool.get("hostname");
        }catch(Exception e) {
            port = 7000;
            hostname = "Master";
            System.out.println("当端口或主机名不正确时，采用默认主机及端口");
        }

        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String delimiter = "\n";
        // 连接socket获取输入的数据
        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimiter);

        DataStream<Tuple2<String, Integer>> windowCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split("\\s");
                for (String word : split) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).keyBy(0) // 使用第一个元素作为key
                .timeWindow(Time.seconds(2), Time.seconds(1))    // 指定时间窗口为2秒，时间间隔为1秒
                .sum(1);    // 使用第二个元素累加
//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> a, Tuple2<String, Integer> b) throws Exception {
//                        return new Tuple2<String, Integer>(a.f0,a.f1+b.f1);
//                    }
//                });

        // 打印数据并设置并行度
        windowCounts.print().setParallelism(1);
        // execute触发程序启动
        env.execute("Socket window count start ...");
    }


}
