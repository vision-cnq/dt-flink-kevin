package com.kevin.scala.transform

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author caonanqing
 * @version 1.0
 * @description     map ： 输入一个元素，输出一个元素
 *             常用于数据集内的数据清洗和转换
 * @createDate 2020/5/16
 */
object MapDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // 方式一，直接传入计算表达式，
    val stream1 = dataStream.map(t => (t._1, t._2 + 1))

    // 方式二：定义map函数逻辑，完成数据处理操作
    val stream2 = dataStream.map(new MapFunction[(String,Int),(String,Int)] {
      override def map(t: (String, Int)): (String, Int) = {
        return (t._1,t._2 + 1)
      }
    })

    // 打印输出
    stream1.print("stream1")
    stream2.print("stream2")

    // 执行程序
    env.execute("MapDemo")
  }

}
