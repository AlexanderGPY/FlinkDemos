package com.gpy.flink.StreamingJob

import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2021/9/8 下午 6:05
  */
object WXWorkTest {
  final val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  def main(args: Array[String]): Unit = {
    import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    val props = new Properties()
    //watermark 允许数据延迟时间
    val maxOutOfOrderness = 86400 * 1000L
    //连接kafka
    props.put("bootstrap.servers", "192.168.248.15:9092")
    props.put("group.id", "flinkHouseGroup")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "earliest")
    props.put("flink.partition-discovery.interval-millis", "30000")
    //消费kafka数据
    val input =
      env.addSource(new FlinkKafkaConsumer010[String]("backend-test", new SimpleStringSchema(), props))
//    val streamData =
//      input
//        .assignTimestampsAndWatermarks(
//          new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(maxOutOfOrderness)) {
//            override def extractTimestamp(element: String): Long = {
//              val t    = JSON.parseObject(element)
//              val time = t.getString("@timestamp-log")
//              time.toLong
//            }
//          }
//        )
//        .map(new mapFunction_1)

    input.print()

    env.execute("Flink Streaming Scala API Skeleton")
  }

  class mapFunction_1 extends MapFunction[String, (String, String)] {
    override def map(value: String): (String, String) = {
      var date = ""
      var ip   = ""
      try {
        val messagePassingJSObject = JSON.parseObject(value)
        val datetime               = DATE_FORMAT.format(messagePassingJSObject.getLong("@timestamp-log"))
        date = datetime.split(" ")(0)
        ip = messagePassingJSObject.getString("ip_addr")
      } catch {
        case ex: Exception => println(value)
      }
      (date, ip)
    }
  }
}
