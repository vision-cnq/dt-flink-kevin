package com.kevin.scala.udf

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author caonanqing
 * @version 1.0
 * @description     实现FilterFunction接口
*              实现udf做更细粒度的控制流
 *                函数类(Function Classes)：flink暴露了所有udf函数的接口(实现方式为接口或者抽象类)，
 *                例如：MapFunction，FilterFunction，ProcessFunction等...
 * @createDate 2020/5/17
 */
object FilterFunctionDemo{

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements("kevin", "coco", "mr.cao")

    // 方式一：定义类实现filter接口函数的udf
    val stream1 = dataStream.filter(new MyFilter("kevin"))

    // 方式二，匿名内部类实现filter的udf
    val stream2 = dataStream.filter(new FilterFunction[String] {
      override def filter(value: String): Boolean = {
        !value.contains("kevin")
      }
    })

    // 打印输出
    stream1.print("stream1")
    stream2.print("stream2")
    // 执行程序
    env.execute("FilterFunctionDemo")

  }

  // 实现FilterFunction接口
  class MyFilter(keyWord: String) extends FilterFunction[String] {
    override def filter(value: String): Boolean = {
      // 过滤掉kevin的数据
      !value.contains(keyWord)
    }
  }
}

