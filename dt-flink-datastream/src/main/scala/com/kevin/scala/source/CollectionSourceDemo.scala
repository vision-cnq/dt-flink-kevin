package com.kevin.scala.source

import java.util.Arrays

import org.apache.flink.streaming.api.scala._

/**
 * @author caonanqing
 * @version 1.0
 * @description   基于集合的source，从集合中读取数据
 *          1.fromElements: 直接获取元素数据，创建dataStream
 *          2.fromCollection： 从集合中获取数据，创建dataStream
 *
 * @createDate 2020/5/13
 */
object CollectionSourceDemo {

  def main(args:Array[String]) : Unit = {

    // 获取运行环境，表示执行程序的上下文，会根据查询运行方式决定返回什么样的运行环境
    // 没有设置并行度时，会根据flink-conf.yaml内的parallelism.default为准，默认为1
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1.通过fromElements从（Tuple）元素集合中创建DataStream数据集
    val dataStream1 = env.fromElements(Tuple2(1L, 3L), Tuple2(1L, 5L), Tuple2(1L, 7L), Tuple2(1L, 4L), Tuple2(1L, 9L))
//    val dataStream1 = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // 2.通过fromElements从element中创建DataStream数据集
    val dataStream2 = env.fromElements("kevin", "coco", "mr.cao")

    // 3.通过fromCollection从List中创建DataStream数据集
    val dataStream3= env.fromCollection(List("kevin","mrcao","mr.cao"))

    // 4.通过自定义集合中读取数据
    val dataStream4 = env.fromCollection(List(
      SensorReading("sensor_1",1547718199,35.8021545),
      SensorReading("sensor_3",1547718201,15.1545),
      SensorReading("sensor_6",1547718202,6.2921545),
      SensorReading("sensor_8",1547718205,38.3021445)
    ) )

    // 打印输出，并且设置并行度
    dataStream1.print("dataStream1").setParallelism(2)
    dataStream2.print("dataStream2").setParallelism(1)
    dataStream3.print("dataStream3").setParallelism(2)
    dataStream4.print("dataStream4").setParallelism(1)

    // 执行程序
    env.execute("SourceDemo")
  }

}

// 温度传感器读数样例类
case class SensorReading(id : String, timestamp : Long, temperature : Double)
