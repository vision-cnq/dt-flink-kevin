package com.kevin.scala.transform

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}

/**
 * @author caonanqing
 * @version 1.0
 * @description       aggregations ： 聚合算子，根据指定的字段进行聚合操作，滚动的产生一系列数据聚合结果。
 *             主要是将reduce算子中的函数进行了封装。
 *             封装的聚合操作包括：
 *              sum : 滚动对key指定的字段进行sum统计
 *              min : 滚动计算key指定的最小值
 *              max : 滚动计算key指定的最大值
 *              minBy : 滚动计算key指定的最小值，返回最小值对应的元素
 *              maxBy : 滚动计算key指定的最大值，返回最大值对应的元素
 * @createDate 2020/5/16
 */
object AggregationsDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // 指定第一个字段为分区key
    val stream : KeyedStream[(String,Int),Tuple] = dataStream.keyBy(0)

    // 滚动对第二个字段进行sum统计
    val sumStream = stream.sum(1)

    // 滚动计算key指定的最小值
    val minStream = stream.min(1)

    // 滚动计算key指定的最大值
    val maxStream = stream.max(1)

    // 滚动计算指定key的最小值，返回最小值对应的元素
    val minByStream = stream.minBy(1)

    // 滚动计算指定key的最大值，返回最大值对应的元素
    val maxByStream = stream.maxBy(1)

    // 打印输出
    sumStream.print("sumStream")
    minStream.print("minStream")
    maxStream.print("maxStream")
    minByStream.print("minByStream")
    maxByStream.print("maxByStream")

    // 执行程序
    env.execute("AggregationsDemo")
  }
}
