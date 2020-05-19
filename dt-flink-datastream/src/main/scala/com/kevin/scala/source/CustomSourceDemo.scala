package com.kevin.scala.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

/**
 * @author caonanqing
 * @version 1.0
 * @description     自定义source
 * @createDate 2020/5/16
 */
object CustomSourceDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    // 读取自定义source的数据
    val dataStream = env.addSource(new SensorSource())

    // 打印输出，并设置并行度
    dataStream.print("dataStream").setParallelism(1)

    // 执行程序
    env.execute("CustomSourceDemo")

  }

}

class SensorSource() extends SourceFunction[SensorReading] {

  // 定义一个flag，表示数据源是否正常运行
  var running : Boolean = true

  // 正常生成数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    // 初始化随机数生成器
    val rand = new Random()

    // 初始化定义一组传感器温度数据，高斯随机生成数据
    var curTemp = 1.to(10).map(
      i => ("sensor_" + i, rand.nextGaussian()*20)
    )

    // 用无线循环，产生数据流
    while(running){
      // 在前一次温度的基础上更新温度值
      curTemp = curTemp.map(
        t => (t._1,t._2 + rand.nextGaussian())
      )
      // 获取当前时间戳
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        t => ctx.collect(SensorReading(t._1,curTime,t._2))
      )

      // 设置时间间隔，观察输出
      Thread.sleep(50)

    }

  }

  // 取消数据源的生成
  override def cancel(): Unit = {
    running = false
  }
}