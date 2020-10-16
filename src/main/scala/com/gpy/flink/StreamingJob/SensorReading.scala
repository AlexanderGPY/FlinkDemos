package com.gpy.flink.StreamingJob

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/8/3 21:14
  */

/**
  * @param id 唯一标识
  * @param timestamp 时间戳
  * @param temperature 温度
  */

case class SensorReading(id: String, timestamp: Long, temperature: Double)
