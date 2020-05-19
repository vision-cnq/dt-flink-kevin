package com.kevin.scala.transform

import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}

/**
 * @author caonanqing
 * @version 1.0
 * @description       iterate : 适合于迭代计算常见，通过每一次的迭代计算，并将计算结果反馈到下一次迭代计算中。
 * @createDate 2020/5/16
 */
object IterateDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(1,5,7,9,2,5,7).map{t :Int => t}

    // 如果事件指标加1=2，则将计算指标反馈到下一次迭代的通道中，否则直接输出到下游dataStream中
    // 其中在执行之前需要对数据集做map处理的主要目的是为了数据分区根据默认并行度进行重平衡
    val stream = dataStream.iterate((input: ConnectedStreams[Int, String]) => {
      // 分别定义两个map方法完成对输入connectedStreams数据集数的处理
      val head = input.map(i => (i + 1).toString, s => s)
      (head.filter(_ == "2"), head.filter(_ != "2"))
      // 1000指定最长迭代等待时间，单位为ms，超过该时间没有数据接入则终止迭代
    }, 1000)

    // 打印输出
    stream.print("stream")

    // 执行程序
    env.execute("IterateDemo")
  }
}
