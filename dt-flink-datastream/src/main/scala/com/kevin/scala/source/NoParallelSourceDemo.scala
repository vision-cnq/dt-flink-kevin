package com.kevin.scala.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author caonanqing
 * @version 1.0
 * @description     自定义NoParallelSource，创建并行度为1的source
 * @createDate 2020/5/17
 */
object NoParallelSourceDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // addSource添加数据源，持续从source输入
    val dataStream = env.addSource(new MyNoParallelSource)

    // 将输入的数据打印
    val mapStream = dataStream.map( line => {
      println("source data: " + line)
      line
    })

    // 对最近两秒内的数据进行sum
    val stream = mapStream.timeWindowAll(Time.seconds(2)).sum(0)

    // 打印输出
    stream.print("stream").setParallelism(1)

    // 执行程序
    env.execute("NoParallelSourceDemo")
  }

  // 自定义NoParallelSource，继承SourceFunction
  class MyNoParallelSource extends SourceFunction[Long]{

    var count = 1
    var isRunning = true

    // 正常生成数据
    override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
      // 从1开始递增数据
      while (isRunning) {
        ctx.collect(count)
        count += 1
        Thread.sleep(1000)
      }
    }

    // 取消生成数据
    override def cancel(): Unit = {
      isRunning = false
    }
  }

}
