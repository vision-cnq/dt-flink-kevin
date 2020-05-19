package com.kevin.scala.transform

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{KeyedStream, StreamExecutionEnvironment}

/**
 * @author caonanqing
 * @version 1.0
 * @description       split ： 将输入的DataStream拆分一个或多个DataStream
 *                    select : 传入已标记好的标签信息，然后将符合条件的数据筛选形成新的数据集。
 *             split函数本身只是对输入数据集进行标记，并没有将数据集真正的实现拆分。因此需要借助select将标记的数据切分成不同的数据集
 * @createDate 2020/5/16
 */
object SplitAndSelectDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // 拆分数据，判断第二个元素%2 == 0
    val splitStream = dataStream.split{ t =>
      if (t._2 % 2 == 0) {
        Seq("偶数")
      } else {
        Seq("奇数")
      }
    }

    // 筛选出偶数数据集
    val stream1 = splitStream.select("偶数")
    // 筛选出奇数数据集
    val stream2 = splitStream.select("奇数")
    // 筛选出奇数与偶数的数据集
    val stream3 = splitStream.select("偶数","奇数")

    // 打印输出
    stream1.print("stream1")
    stream2.print("stream2")
    stream3.print("stream3")

    // 执行程序
    env.execute("SplitAndSelectDemo")
  }
}
