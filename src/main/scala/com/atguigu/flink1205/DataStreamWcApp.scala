package com.atguigu.flink1205

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

object DataStreamWcApp {


  def main(args: Array[String]): Unit = {
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val dataStream: DataStream[String] = env.socketTextStream("hadoop1",7777)

        val sumDstream: DataStream[(String, Int)] = dataStream.flatMap(_.split(" ")).filter(_.nonEmpty).map((_,1)).keyBy(0).sum(1)

         sumDstream.print()

        env.execute()



  }

}
