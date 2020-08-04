package com.gpy.flink.StreamingJob

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/8/3 21:14
  */

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * Skeleton for a Flink Streaming Job.
  *
  * For a tutorial how to write a Flink streaming application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution, run
  * 'mvn clean package' on the command line.
  *
  * If you change the name of the main class (with the public static void main(String[] args))
  * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
  */
object SocketWindowWordCount {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
    //implicits
    import org.apache.flink.api.scala._

    //parse the data
    val WC = text
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      //.timeWindow(Time.seconds(5), Time.seconds(1))
      .sum(1)

    //print
    WC.print().setParallelism(1)

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
