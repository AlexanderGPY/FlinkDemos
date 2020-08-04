package com.gpy.flink.StreamingJob

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/8/3 21:14
  */

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{createTypeInformation, DataStream, StreamExecutionEnvironment}

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

object StreamingJob {
  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置并行度

    //获取数据源数据
    val stream: DataStream[SensorReading] = env.addSource(new SensorSource)

    //stream.print()

    //解析数据

    //spark算子写法

    /*    val wordCount = stream
          .map(x => {
            (x.id, x.temperature)
          })
          .keyBy(0)
          .sum(1)
        wordCount.print()*/

    //flink算子写法，自定义mapfunction

    stream.map(new MyMapFunction).keyBy(0).sum(1).print()

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }

  //自定义MapFunction
  class MyMapFunction extends MapFunction[SensorReading, (String, Double)] {
    override def map(t: SensorReading): (String, Double) = {
      (t.id, t.temperature)
    }
  }
}
