package com.kevin.scala.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
 * @author caonanqing
 * @version 1.0
 * @description     基于kafka的source，读取kafka的数据
 * @createDate 2020/5/16
 */
object KafkaSourceDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 配置kafka
    val prop = new Properties()
    prop.setProperty("bootstrap.servers","Slave3:9092")
    prop.setProperty("group.id","consumer-group")
    prop.put("auto.offset.reset", "latest")
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    import org.apache.flink.api.scala._

    // 读取kafka中topic为sensor的数据
    val dataStream = env.addSource(new FlinkKafkaConsumer010[String]("sensor", new SimpleStringSchema(), prop))

    // 打印输出设置并行度
    dataStream.print("dataStream").setParallelism(1)

    // 执行程序
    env.execute("KafkaSourceDemo")

  }

}
