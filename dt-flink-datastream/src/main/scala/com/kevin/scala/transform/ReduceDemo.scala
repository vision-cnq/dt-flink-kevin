package com.kevin.scala.transform

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}

/**
 * @author caonanqing
 * @version 1.0
 * @description     reduce ： 输入的元素通过计算方式滚动地进行数据聚合处理
 *     flink保存累计值：
 *        flink是一种有状态的流计算框架，状态包括两个层面
 *         1.operator state 主要是保存数据在流程中的处理状态，用于确保语义的exactly-once
 *         2.keyed state 主要是保存数据在计算过程中的累计值
 *     这两种状态都是可以通过checkpoint机制保存在StageBackend中，StateBackend可以选择保存在内存中（默认）或者保存在磁盘文件中。
 * @createDate 2020/5/16
 */
object ReduceDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // 指定第一个字段为分区key
    val stream : KeyedStream[(String,Int),Tuple] = dataStream.keyBy(0)

    // 合并当前元素与上次聚合的值
    val reduceStream = stream.reduce { (t1, t2) => (t1._1, t1._2 + t2._2) }

    // 打印输出
    reduceStream.print("reduceStream")

    // 执行程序
    env.execute("ReduceDemo")
  }

}
