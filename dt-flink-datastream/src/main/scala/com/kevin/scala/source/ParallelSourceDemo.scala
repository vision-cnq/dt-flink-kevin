package com.kevin.scala.source

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author caonanqing
 * @version 1.0
 * @description     自定义ParallelSource，创建并行度为1的source
 *             ParallelSourceFunction: 定义并发数据源接入器，
 *             ParallelSourceFunction继承了SourceFunction接口，并没有定义其他额外的方法，仅仅是用接口名来表达意图，即可以被并行执行的stream data source
 *
 * @createDate 2020/5/17
 */
object ParallelSourceDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // addSource添加数据源，持续从source输入
    val dataStream = env.addSource(new MyParallelSource).setParallelism(2)

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
    env.execute("ParallelSourceDemo")
  }


  // 自定义ParallelSource，继承ParallelSourceFunction
  class MyParallelSource extends ParallelSourceFunction[Long]{

    var count = 1L
    var isRunning = true

    // 正常生成数据
    override def run(ctx: SourceContext[Long]) = {
      // 从1开始递增数据
      while(isRunning){
        ctx.collect(count)
        count+=1
        Thread.sleep(1000)
      }

    }

    // 取消生成数据
    override def cancel() = {
      isRunning = false
    }
  }

}
