package com.kevin.scala

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * 批处理单词计数
  * @author caonanqing on createDate 2020/2/27
  */
object BatchWordCountScala {

  def main(args: Array[String]): Unit = {

    // 1.获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2.创建数据集
    val text = env.fromElements("java java scala","scala java python")
    // 3.flatMap将数据转成大写并以空格进行分割，且过滤掉空
    // map进行单词计数，groupBy归纳相同的key，sum将value相加
    val counts = text.flatMap{ _.toLowerCase.split(" ") filter { _.nonEmpty }}
      .map{ (_,1)}
      .groupBy(0)
      .sum(1)

    // 4.打印
    counts.print()

  }

}
