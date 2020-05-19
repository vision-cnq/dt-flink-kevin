package com.kevin.scala.source

import com.kevin.scala.source.ParallelSourceDemo.MyParallelSource
import org.apache.flink.configuration.Configuration 
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author caonanqing
 * @version 1.0
 * @description     自定义RichParallelSource，创建并行度为1的source
 *             RichParallelSourceFunction: 定义并发数据源接入器
 *             RichParallelSourceFunction实现了ParallelSourceFunction接口，同时继承了AbstractRichFunction
 *             AbstractRichFunction主要实现了RichFunction接口的setRuntimeContext、getRuntimeContext、getIterationRuntimeContext方法；open及close方法都是空操作
 *                  RuntimeContext定义了很多方法:
 *                      getNumberOfParallelSubtasks方法，它可以返回当前的task的parallelism；
 *                      getIndexOfThisSubtask则可以获取当前parallel subtask的下标；
 *                      根据这些信息，开发并行执行且各自发射的数据又不重复的ParallelSourceFunction
 *
 * @createDate 2020/5/17
 */
object RichParallelSourceDemo {

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
    env.execute("RichParallelSourceDemo")
  }

  // 自定义RichParallelSource,继承RichParallelSourceFunction函数
  class MyRichParallelSourceScala extends RichParallelSourceFunction[Long]{

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

    override def open(parameters: Configuration): Unit = super.open(parameters)
    override def close(): Unit = super.close()
  }

}
