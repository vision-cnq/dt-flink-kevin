package com.kevin.scala.transform

import org.apache.flink.api.common.functions.{MapFunction, Partitioner}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author caonanqing
 * @version 1.0
 * @description     物理分区 ：操作的作用是根据指定的分区策略将数据重新分配到不同节点的task实例上运行
 *             常见的分区策略：
 *             随机分区 : shuffle, 通过随机的方式将数据分配在下游算子的每个分区中，分区相对均衡，但是比较容易失去原有数据的分区结构。
 *             按比列分区 : rebalance, 通过循环的方式对数据集中的数据进行重分区，能够尽快保证每个分区的数据平衡，
 *                        当数据集发生数据倾斜的时候使用这种策略就是比较有效的优化方法。
 *             平衡分区 : rescale, 仅仅对上下游教程的算子数据进行重平衡，具体的分区主要根据上下游算子的并行度决定。
 *             广播操作 : broadcast, 广播策略将输入的数据集复制到下游算子的并行Tasks实例中，下游算子中的Tasks可以直接从本地内存中获取广播数据集，不再依赖网络传输。
 *                        这个分区策略适合小数据集，例如：大数据集关联小数据集时，可以通过广播的方式将小数据集分发到算子的每个分区中。
 *             自定义分区 : 实现自定义分区器，需要继承Partitioner，然后调用dataStreamAPI上partitionCustom()方法将创建的分区应用到数据集上。
 *
 * @createDate 2020/5/16
 */
object PhysicalPartitioningDemo {

  def main(args: Array[String]): Unit = {

    // 获取运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    // 创建数据集
    val stream = env.fromElements(("a", 3), ("b", 5), ("c", 1), ("a", 9), ("c", 7), ("a", 8), ("d", 2))

    // shuffle实现数据集的随机分区
    val shuffle = stream.shuffle

    // rebalance()方法实现数据的重平衡分区
    val rebalance = stream.rebalance

    // rescale()实现rescaling partitioning操作
    val rescale = stream.rescale

    // broadcast()方法实现广播分区
    val broadcast = stream.broadcast

    // 自定义分区策略，通过数据集字段所以指定分区字段
    val partitioner = stream.partitionCustom(customPartitioner,0)

    // 打印输出
    shuffle.print("shuffle")
    rebalance.print("rebalance")
    rescale.print("rescale")
    broadcast.print("broadcast")
    partitioner.print("partitioner")

    // 执行程序
    env.execute("PhysicalPartitioningDemo")
  }

  // 自定义分区
  object customPartitioner extends Partitioner[String] {
    // 获取随时数生成器
    val r = scala.util.Random
    override def partition(key: String, numPartitions: Int): Int = {
      // 自定义分区策略，key中如果包含a则放在0分区，其他情况则根据Partitions num随机分区
      if(key.contains("flink"))
        0
      else
        r.nextInt(numPartitions)
    }
  }
}
