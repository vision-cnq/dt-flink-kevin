package com.kevin.scala.source

import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._

/**
 * @author caonanqing
 * @version 1.0
 * @description     基于文件的source，读取文件中的数据，并转换成分布式数据集dataStream进行数据处理
 *            readTextFile: 直接读数文本文件。
 *            readFile: 可以使用readFile方法通过指定InputFormat来读取特定数据类型的文件。
 *
 * @createDate 2020/5/13
 */
object FileSourceDemo {

  def main(args:Array[String]) : Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

	  // 1.通过readTextFile，读取Text文件
    val dataStream1 = env.readTextFile("dt-flink-datastream\\src\\main\\resources\\goods_click_data.txt")

    // 2.通过readTextFile，读取hdfs文件
    val dataStream2 = env.readTextFile("hdfs://Master:9000/words/word.txt")

    // 3.通过指定CSVInputFormat读取CSV文件
    val dataStream3 = env.readFile(new CsvInputFormat[String](new Path("dt-flink-datastream\\src\\main\\resources\\goods_click_data.csv")) {
      override def fillRecord(out: String, objects: Array[AnyRef]): String = {
        return null
      }
    }, "dt-flink-datastream\\src\\main\\resources\\goods_click_data.csv")

    // 打印输出
    dataStream1.print("dataStream1").setParallelism(1)
    dataStream2.print("dataStream2").setParallelism(1)
    dataStream3.print("dataStream3").setParallelism(1)

    // 启动
    env.execute()

  }

}


