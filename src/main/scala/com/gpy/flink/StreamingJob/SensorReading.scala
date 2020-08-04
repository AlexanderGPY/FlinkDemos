package com.gpy.flink.StreamingJob

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/8/3 21:14
  */

/**
  * @param id
  * @param timestamp
  * @param temperature
  */

case class SensorReading(id: String, timestamp: Long, temperature: Double)
