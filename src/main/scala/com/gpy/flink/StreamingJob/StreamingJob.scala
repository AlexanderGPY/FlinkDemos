package com.gpy.flink.StreamingJob

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/8/3 21:14
  */

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{createTypeInformation, DataStream, KeyedStream, StreamExecutionEnvironment}

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

    //匿名函数风格 map算子写法
    /*stream.map(x => (x.id, x.temperature)).keyBy(0).sum(1).print()*/

    //flink风格 map算子写法，自定义mapfunction
    stream.map(new MyMapFunction).keyBy(0).sum(1).print()

    //匿名函数风格 reduce算子写法
    //stream.map(new MyMapFunction).keyBy(0).reduce((x, y) => (x._1, x._2 + y._2)).print()

    //flink风格 reduce算子写法
    /*stream.map(new MyMapFunction).keyBy(0).reduce(new MyReduceFunction).print()*/

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }

  //自定义MapFunction
  class MyMapFunction extends MapFunction[SensorReading, (String, Double)] {
    override def map(t: SensorReading): (String, Double) = {
      (t.id, t.temperature)
    }
  }
  //实现reduceFunction
  class MyReduceFunction extends ReduceFunction[(String, Double)] {
    override def reduce(value1: (String, Double), value2: (String, Double)): (String, Double) = {
      (value1._1, value1._2 + value2._2)
    }
  }
  //实现CoMapFunction
  //class MyCoMapFunction extends CoMapFunction[]

}
