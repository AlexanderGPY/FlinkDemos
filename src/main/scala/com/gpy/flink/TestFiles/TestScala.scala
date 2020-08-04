package com.gpy.flink.TestFiles

/**
  * @description:
  * @author:AlexanderGuo
  * @date:2020/8/4 17:58
  */
object TestScala {
  def main(args: Array[String]): Unit = {
    val a = BigDecimal(1.03)
    val b = BigDecimal(1.44)

    val c = a + b

    println(BigDecimal(1.03) + BigDecimal(1.44))
    println(c)
  }
}
