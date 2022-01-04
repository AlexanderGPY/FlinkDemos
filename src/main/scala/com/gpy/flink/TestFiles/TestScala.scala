package com.gpy.flink.TestFiles

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/8/4 17:58
  */
object TestScala {
  def main(args: Array[String]): Unit = {
    //创建一个批处理执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //从文件中读取数据

    val inputPath =
      "D:\\ideaProject\\flinkdemo\\src\\main\\resources\\hello.txt"
    val inputDataSet: DataSet[String] = env.readTextFile(inputPath)

    //对数据进行转换处理
    //先分词
    //按word进行分组
    //聚合统计
    val resultDataSet: DataSet[(String, Int)] =
      inputDataSet.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)

    resultDataSet.print()

  }
}
