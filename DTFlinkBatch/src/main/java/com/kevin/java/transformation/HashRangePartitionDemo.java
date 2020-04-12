package com.kevin.java.transformation;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author caonanqing
 * @version 1.0
 * @description
 *      HashPartitioner分区原理：对于给定的key，计算其hashCode，并除以分区的个数取余，如果余数小于0，则用余数加分区的个数，最后返回的值就是这个key的所属分区id。
 *      RangePartitioner分区原理：将一定范围内的数据映射到某一个分区内，分界算法对应的函数是rangeBounds。
 *      RangePartitioner分区的优势：可以解决HashPartitioner分区中可能出现的数据不均匀现象。
 * @createDate 2020/3/9
 */
public class HashRangePartitionDemo {

    public static void main(String[] args) throws Exception {
        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ArrayList<Tuple2<Integer, String>> data = new ArrayList<>();
        data.add(new Tuple2<>(1,"hello1"));
        data.add(new Tuple2<>(2,"hello2"));
        data.add(new Tuple2<>(2,"hello3"));
        data.add(new Tuple2<>(3,"hello4"));
        data.add(new Tuple2<>(3,"hello5"));
        data.add(new Tuple2<>(3,"hello6"));
        data.add(new Tuple2<>(4,"hello7"));
        data.add(new Tuple2<>(4,"hello8"));
        data.add(new Tuple2<>(4,"hello9"));
        data.add(new Tuple2<>(4,"hello10"));
        data.add(new Tuple2<>(5,"hello11"));
        data.add(new Tuple2<>(5,"hello12"));
        data.add(new Tuple2<>(5,"hello13"));
        data.add(new Tuple2<>(5,"hello14"));
        data.add(new Tuple2<>(5,"hello15"));
        data.add(new Tuple2<>(6,"hello16"));
        data.add(new Tuple2<>(6,"hello17"));
        data.add(new Tuple2<>(6,"hello18"));
        data.add(new Tuple2<>(6,"hello19"));
        data.add(new Tuple2<>(6,"hello20"));
        data.add(new Tuple2<>(6,"hello21"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        // 根据给定的key对一个数据集进行hash分区
        text.partitionByHash(0).mapPartition(new MapPartitionFunction<Tuple2<Integer,String>, Tuple2<Integer,String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> it = values.iterator();
                while (it.hasNext()){
                    Tuple2<Integer, String> next = it.next();
                    System.out.println("当前线程id："+Thread.currentThread().getId()+","+next);
                }
            }
        }).print();

        System.out.println("----------------");

        // 根据给定的key对一个数据集进行范围分区
        text.partitionByRange(0).mapPartition(new MapPartitionFunction<Tuple2<Integer,String>, Tuple2<Integer,String>>() {
            @Override
            public void mapPartition(Iterable<Tuple2<Integer, String>> values, Collector<Tuple2<Integer, String>> out) throws Exception {
                Iterator<Tuple2<Integer, String>> it = values.iterator();
                while (it.hasNext()){
                    Tuple2<Integer, String> next = it.next();
                    System.out.println("当前线程id: "+Thread.currentThread().getId()+" , "+next);
                }
            }
        }).print();


    }
}
