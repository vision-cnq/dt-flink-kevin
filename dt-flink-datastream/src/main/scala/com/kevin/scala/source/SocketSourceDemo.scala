package com.kevin.scala.source

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author caonanqing
 * @version 1.0
 * @description     基于socket的source，读取Socket源源不断输入的数据
 *
 * @createDate 2020/5/17
 */
object SocketSourceDemo {

  def main(args:Array[String]) : Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 获取Socket接入数据，设置ip和端口
    // 在master端可以开启 nc -lk 7000，进行输入数据测试
    val dataStream = env.socketTextStream("master",7000)

    // 打印输出
    dataStream.print("dataStream").setParallelism(1)
    // 执行程序
    env.execute("SocketSourceDemo")
  }
}
