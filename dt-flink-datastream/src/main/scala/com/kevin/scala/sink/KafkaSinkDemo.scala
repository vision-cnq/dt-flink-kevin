package com.kevin.scala.sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer010}

/**
 * @author caonanqing
 * @version 1.0
 * @description     基于第三方的sink，将数据输出到kafka
 * @createDate 2020/5/17
 */
object KafkaSinkDemo {

  def main(args:Array[String]) : Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    // 创建数据集
    val dataStream = env.fromElements("kevin","coco","mr.cao")

    // 指定Broker List参数
    val broker = "Slave3:9092"
    // 指定目标Kafka Topic名称
    val topic = "kafka-topic"
    // 通过writeAsText方法将数据直接输出到本地文件系统
    val kafkaProducer = new FlinkKafkaProducer010[String](broker, topic, new SimpleStringSchema)
    // 通过addSink添加kafkaProducer到算子拓扑中
    dataStream.addSink(kafkaProducer)
    // 开启的kafka消费者测试： ./bin/kafka-console-consumer.sh --zookeeper master:2181 --topic kafka-topic --from-beginning

    // 执行程序
    env.execute("KafkaSinkDemo")
  }
}
