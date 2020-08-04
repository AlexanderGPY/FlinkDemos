package com.gpy.flink.StreamingJob

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/8/3 21:14
  */

import java.lang
import java.lang.Thread
import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction

import scala.util.Random

//case class SensorReading(id: String, timestamp: Long, temperature: Double)

class SensorSource extends RichParallelSourceFunction[SensorReading] {

  //flag表示数据源是否还在正常运行
  var running: Boolean = true

  //run函数连续发送SensorReading数据
  override def run(srcCtx: SourceContext[SensorReading]): Unit = {
    //初始化随机数发生器
    val rand = new Random()

    //查找上下文任务索引
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    //初始化10个温度传感器元组

    var curFTemp = (1 to 10).map {
      //高斯随机数
      i => ("sensor_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() + 20))
    }
    //无限循环 产生数据流
    while (running) {
      //更新温度
      curFTemp = curFTemp.map(t => (t._1, t._2 + (rand.nextGaussian() * 0.5)))
      //获取当前时间戳

      val curTime = Calendar.getInstance.getTimeInMillis

      //发射新的传感器数据，注意这里的srcCtx.collect

      curFTemp.foreach(t => srcCtx.collect(SensorReading(t._1, curTime, t._2)))
      //每一百毫秒发射一次
      Thread.sleep(100)
    }
  }

  //可控制的退出操作
  override def cancel(): Unit = {
    running = false
  }

}
