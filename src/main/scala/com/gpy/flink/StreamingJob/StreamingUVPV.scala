package com.gpy.flink.StreamingJob

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.api.scala.StreamTableEnvironment

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/10/16 下午 5:49
  */
object StreamingUVPV {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tEnv = StreamTableEnvironment.create(env)
    val port =
      try {
        11111
      } catch {
        case e: Exception =>
          System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
          return
      }
    //get data from socket format dataStream
    val text: DataStream[String] = env.socketTextStream("localhost", port, '\n')
    //implicits,不导入隐式转换会flatMap失败
    import org.apache.flink.api.scala._
    //parse the data

    //print

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
