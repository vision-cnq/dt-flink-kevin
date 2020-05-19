package com.kevin.scala.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author caonanqing
 * @version 1.0
 * @description     基于print的sink，将数据输出到console
 * @createDate 2020/5/17
 */
object PrintSinkDemo {

  def main(args:Array[String]) : Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // 打印输出到console
    dataStream.print("dataStream")

    // 执行程序
    env.execute("PrintSinkDemo")
  }
}
