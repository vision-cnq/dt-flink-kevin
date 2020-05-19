package com.kevin.scala.transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author caonanqing
 * @version 1.0
 * @description   flatMap : 输入一个元素，输出一个或多个元素
 *
 * @createDate 2020/5/16
 */
object FlatMapDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromCollection(List("kevin cao","mrcao coco","mr.cao kevin"))

    // 切分元素
    val stream = dataStream.flatMap{ str => str.split(" ") }

    // 打印输出
    stream.print("stream")

    // 执行程序
    env.execute("FlatMapDemo")
  }
}
