package com.kevin.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author caonanqing
 * @version 1.0
 * @description     brocadcast广播变量
 *      从数据源中获取用户的姓名，最终将用户的姓名与年龄输出
 *      在中间的map处理时获取用户的年龄信息，将用户的关系数据集使用广播变量进行处理
 *
 *      注意：如果多个算子需要使用同一份数据集，那么需要在对应的多个算子后面分别注册广播变量
 *
 * @createDate 2020/3/10
 */
public class BroadcastDemo {

    public static void main(String[] args) throws Exception {

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 需要广播的数据，一般将广播数据放在配置文件中
        ArrayList<Tuple2<String, Integer>> list = new ArrayList<>();
        list.add(new Tuple2<>("kevin",23));
        list.add(new Tuple2<>("coco",20));
        list.add(new Tuple2<>("Mr.Cao",24));

        // 将数据转成DataSet
        DataSource<Tuple2<String, Integer>> broadCastData = env.fromCollection(list);

        // 处理需要广播的数据，把数据集转换成map类型，map中的key就是用户姓名，value就是用户年龄
        DataSet<HashMap<String, Integer>> toBroadCast = broadCastData.map(new MapFunction<Tuple2<String, Integer>, HashMap<String, Integer>>() {
            @Override
            public HashMap<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                HashMap<String, Integer> map = new HashMap<>();
                map.put(value.f0, value.f1);
                return map;
            }
        });

        // 源数据
        DataSource<String> data = env.fromElements("kevin");

        // 使用RichMapFunction获取广播变量
        MapOperator<String, String> result = data.map(new RichMapFunction<String, String>() {
            List<HashMap<String, Integer>> broadCastMap = new ArrayList<>();
            HashMap<String, Integer> allMap = new HashMap<>();
            // 该方法只会执行一次，可以用来实现一些初始化的功能，比如open方法中获取广播变量数据
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 获取广播数据
                this.broadCastMap = getRuntimeContext().getBroadcastVariable("broadCastMapName");
                for (HashMap map : broadCastMap) {
                    allMap.putAll(map);
                }
            }

            @Override
            public String map(String value) throws Exception {
                Integer age = allMap.get(value);
                return value + "," + age;
            }
        }).withBroadcastSet(toBroadCast, "broadCastMapName");// 执行广播数据的操作

        // 输出用户姓名,年龄
        result.print();

    }

}
