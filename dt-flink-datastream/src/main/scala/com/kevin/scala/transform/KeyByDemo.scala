package com.kevin.scala.transform

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}

/**
 * @author caonanqing
 * @version 1.0
 * @description     KeyBy : 将指定的key指定partition操作，进行分组
 *             以下两种数据类型不能使用KeyBy方法对数据集进行重分区
 *              1.POJOs类型数据,没有复写hashCode(),只是依赖Object.hash()
 *              2.任何数据类型的数组结构
 *
 * @createDate 2020/5/16
 */
object KeyByDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // 指定第一个字段为分区key
    val stream : KeyedStream[(String,Int),Tuple] = dataStream.keyBy(0)

    // 打印输出
    stream.print("stream")

    // 执行程序
    env.execute("KeyByDemo")
  }
}
