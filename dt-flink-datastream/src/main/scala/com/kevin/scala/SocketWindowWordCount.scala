package com.kevin.scala

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author caonanqing
 * @version 1.0
 * @description   实现对最近5秒内的数据进行汇总计算
 * @createDate 2020/5/13
 */
object SocketWindowWordCount {

  def main(args:Array[String]) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("master",7000)

    val counts = text.flatMap{_.toLowerCase.split("\\W+")filter{_.nonEmpty}}
      .map{(_,1)}
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()

    env.execute("SocketWindowWordCount")
  }

}
