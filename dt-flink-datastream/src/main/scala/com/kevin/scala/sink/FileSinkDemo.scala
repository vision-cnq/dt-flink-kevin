package com.kevin.scala.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author caonanqing
 * @version 1.0
 * @description     基于file的sink，将数据输出到文件中
 *             writeAsCsv: 通过writeAsCsv方法将数据转换成Csv文件输出，并执行输出模式为OVERWRITE。
 *             writeAsText: 通过writeAsText方法将数据直接输出到本地文件系统。
 *             writeToSocket: 通过writeToSocket方法将dataStream数据集输出到指定的socket端口。
 *             writeAsText: 通过writeAsText方法将数据直接输出到hdfs分布式文件系统。
 *
 * @createDate 2020/5/17
 */
object FileSinkDemo {

  def main(args:Array[String]) : Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))
    val dataStream1 = env.fromElements("kevin","coco","mr.cao")

    // 通过writeAsCsv方法将数据转换成Csv文件输出，并执行输出模式为OVERWRITE，csv需要使用Tuple格式
    dataStream.writeAsCsv("dt-flink-datastream\\src\\main\\resources\\sink1.csv",WriteMode.OVERWRITE)

    // 通过writeAsText方法将数据直接输出到本地文件系统
    dataStream.writeAsText("dt-flink-datastream\\src\\main\\resources\\sink2.txt")

    // 通过writeAsText方法将数据直接输出到hdfs分布式文件系统
    dataStream.writeAsText("hdfs://Master:9000//words//sink3.txt")

    // 通过writeToSocket方法将dataStream数据集输出到指定的socket端口，先到master开启 nc -l 7000，监听端口数据，如果要输入到文件，nc -l 7000 > text.txt
    dataStream1.writeToSocket("master",7000,new SimpleStringSchema())

    // 执行程序
    env.execute("FileSinkDemo")
  }

}
