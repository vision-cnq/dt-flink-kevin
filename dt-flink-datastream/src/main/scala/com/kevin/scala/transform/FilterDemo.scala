package com.kevin.scala.transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author caonanqing
 * @version 1.0
 * @description   filter : 对输入的元素进行筛选，过滤不符合条件的元素
 * @createDate 2020/5/16
 */
object FilterDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // 过滤输出第二个元素为1的值
    val stream = dataStream.filter( x => x._2 == 1)

    // 打印输出
    stream.print("stream")

    // 执行程序
    env.execute("FilterDemo")
  }
}
