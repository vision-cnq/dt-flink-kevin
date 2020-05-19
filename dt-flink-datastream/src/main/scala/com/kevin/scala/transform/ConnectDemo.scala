package com.kevin.scala.transform

import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author caonanqing
 * @version 1.0
 * @description       connect : 合并两种或多种不同数据类型的数据集，合并后会保留原来数据集的数据类型。
 *             连接操作允许共享状态数据，就是多个数据集之间可以操作和查看对方数据集的状态。
 *
 *             注1：connectedStreams类型的数据集不能直接进行类似的print()的操作，再转换成dataStream类型数据集，
 *                 在flink提供的map()方法和flatMap()需要CoMapFunction或CoFlatMapFunction分别处理输入的dataStream数据集，或直接传入两个MapFunction来分别处理两个数据集。
 *             注2：coMapFunction和CoFlatMapFunction在Paralism>1的情况下，不会按照指定的顺序执行，可能会影响输出数据的顺序和结果。
 *
 *             通常情况下，coMapFunction和coFlatMapFunction无法有效的解决数据集关联的问题，如果想通过制定的条件对两个数据集进行关联，然后产生相关性较强的结果数据集，
 *              使用keyBy或broadcast广播变量会更好
 * @createDate 2020/5/16
 */
object ConnectDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集1
    val dataStream1 = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))
    // 创建数据集2
    val dataStream2 = env.fromElements(1, 2, 3, 4, 5, 6, 7)

    // 连接两个DataStream数据集
    val connectStream = dataStream1.connect(dataStream2)

    // 使用map(),实现map1和map2方法，分别处理输入两个数据集，同时两个方法返回的数据类型必须一致
    // 两个函数会多线程交替执行产生结果，最终将两个结果集根据定义合并成目标数据集
    val coMapStream = connectStream.map(new CoMapFunction[(String, Int), Int, (Int, String)] {
      // 定义第一个数据集函数处理逻辑，输入值为第一个dataStream
      override def map1(in1: (String, Int)): (Int, String) = {
        (in1._2, in1._1)
      }

      // 定义第二个函数处理逻辑，输入值为第二个dataStream
      override def map2(in2: Int): (Int, String) = {
        (in2, "default")
      }
    })

    // 使用flatMap()
    val coFlatMapStream = connectStream.flatMap(new CoFlatMapFunction[(String,Int),Int,(String,Int,Int)] {
      // 定义共享变量
      var number = 0
      // 定义第一个数据集处理函数
      override def flatMap1(in1: (String, Int), out: Collector[(String, Int, Int)]): Unit = {
        out.collect((in1._1,in1._2,number))
      }
      // 定义第二个数据集处理函数
      override def flatMap2(in2: Int, out: Collector[(String, Int, Int)]): Unit = {
        number = in2
      }
    })

    // 打印输出
    coMapStream.print("coMapStream")
    coFlatMapStream.print("coFlatMapStream")

    // 执行程序
    env.execute("ConnectDemo")
  }
}
