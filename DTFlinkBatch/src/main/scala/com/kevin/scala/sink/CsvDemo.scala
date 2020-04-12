package com.kevin.scala.sink

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * @author caonanqing
 * @version 1.0
 * @description   将结果输出到csv文件中
 * @createDate 2020/4/12
 */
object CsvDemo {

  def main(args:Array[String]):Unit = {

    val inputPath = "DTFlinkBatch\\src\\main\\resources\\words.txt"
    val outPath = "D:\\big_data\\result1"
    // 获取运行环境
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(inputPath)
    // 引用隐式转换
    import org.apache.flink.api.scala._
    // 转换成小写，并切分，过滤掉为空值，将单词数值初始化为1，第一个元素作为分组，第二个元素进行求和相加
    val counts = text.flatMap(_.toLowerCase.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1)
    // 将结果数据保存到csv中，每行以\n切分，每列以空格切分，设置分区为2个（输出到两个文件）
    counts.writeAsCsv(outPath,"\n"," ").setParallelism(2)
    // 执行程序
    env.execute("csv demo")

  }

}
