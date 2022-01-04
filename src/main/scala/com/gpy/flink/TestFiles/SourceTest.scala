package com.gpy.flink.TestFiles

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/9/17 18:21:55
  */

object SourceTest {

  case class SensorReading(id: String, timestamp: Long, temperature: Double)
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //1、从集合读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_2", 1547718201, 35.9),
      SensorReading("sensor_3", 1547718202, 35.7),
      SensorReading("sensor_4", 1547718205, 35.6)
    )

    val stream1 = env.fromCollection(dataList)

    //2、从kafka读取数据
    val props = new Properties()

    //连接kafka
    props.put("bootstrap.servers", "192.168.248.15:9092")
    props.put("group.id", "WXWorkConsumer_test")
    props.put("group.id", "flinkHouseGroup")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("flink.partition-discovery.interval-millis", "30000")
    //消费kafka数据
    val stream2 = env.addSource(new FlinkKafkaConsumer010[String]("frontend-monitor", new SimpleStringSchema(), props))

    env.execute("source test")
  }
}
