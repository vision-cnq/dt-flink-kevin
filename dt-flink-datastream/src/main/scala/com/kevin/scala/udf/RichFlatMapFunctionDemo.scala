package com.kevin.scala.udf

import org.apache.flink.api.common.functions.{RichFlatMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

/**
 * @author caonanqing
 * @version 1.0
 * @description       富函数(Rich Functions)：是DataStream API提供的一个函数类的接口，所有的flink函数类都有其Rich版本。
*                            与常规函数不同的是，可以获取运行环境的上下文，并拥有一些生命周期，可以实现更复杂的功能
 *              例如：RichMapFunction，RichFlatMapFunction，RichFilterFunction等...
 *
 *              Rich Function有一个生命周期的概念，典型的生命周期方法有：
 *                open: open()方法是rich function的初始化方法，当一个算子被调用之前open()会被调用。
 *                close: close()方法是生命周期中最后一个调用的方法，做一些清理工作，
 *                getRuntimeContext: getRuntimeContext()方法提供了函数的getRuntimeContext的一些信息，例如函数执行的并行度，任务的名字，state状态等。
 *
 * @createDate 2020/5/17
 */
object RichFlatMapFunctionDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(1,5,7,9,2,5,7)

    // 实现自定义的flatMap
    val stream = dataStream.flatMap(new MyFlatMap)

    // 打印输出
    stream.print("stream")
    // 执行程序
    env.execute("RichFlatMapFunctionDemo")

  }

  // 自定义一个FlatMap富函数，实现RichFlatMapFunction接口
  class MyFlatMap extends RichFlatMapFunction[Int,(Int,Int)]{

    // 当前子任务编号
    var subTaskIndex = 0

    // 复写open
    override def open(parameters: Configuration): Unit = {
      // 通过上下本环境，获取当前子任务编号
      subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
      // 可以做一些初始化操作，比如建立hdfs连接、数据库连接等
    }

    // 复写close
    override def close(): Unit = {
      // 做一些清理的工作，比如端口hdfs的连接、或数据库连接等
    }

    // 复写flatMap函数
    override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
      // 判断in%2是否等于0
      if(in % 2 == 0){
        out.collect((subTaskIndex,in))
      }
    }
  }

}
