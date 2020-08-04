package com.gpy.flink.StreamingJob

/**
 * @param id
 * @param timestamp
 * @param temperature
 */


case class SensorReading(id: String, timestamp: Long, temperature: Double)