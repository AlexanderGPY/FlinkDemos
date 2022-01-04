package com.gpy.flink.TestFiles

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/9/16 19:08:01
  */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val paramTool       = ParameterTool.fromArgs(args)
    val host            = paramTool.get("host")
    val port            = paramTool.getInt("port")
    val inputDataStream = env.socketTextStream(host, port)

    val resultDataStream: DataStream[(String, Int)] =
      inputDataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_, 1)).keyBy(0).sum(1)

    resultDataStream.print()

    //启动任务
    env.execute("StreamWordCount")
  }
}
