package com.kevin.scala

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * 流处理的wordcount
  * @author caonanqing on createDate 2020/3/12
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    // 从命令行中获取参数
    val tool:ParameterTool = ParameterTool.fromArgs(args)
    val host:String = tool.get("host")
    val port:Int = tool.get("port").toInt

    // 创建流处理环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收socket文本流
    val textDstream:DataStream[String] = env.socketTextStream(host,port)
    // flatMap和Map需要引用隐式转换
    import org.apache.flink.api.scala._
    // 处理：分组并且sum聚合
    val dStream:DataStream[(String,Int)] = textDstream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)
    // 打印输出
    dStream.print()
    // 启动
    env.execute()

  }

}
