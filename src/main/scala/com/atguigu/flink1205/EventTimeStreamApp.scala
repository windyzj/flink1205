package com.atguigu.flink1205

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

import scala.collection.mutable
object EventTimeStreamApp {

  def main(args: Array[String]): Unit = {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //时间特性
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      val textDstream: DataStream[String] = env.socketTextStream("hadoop1",7777)
    textDstream.print("text====>")
    // k1 1000
    //k1 2000
    val textTupleDstream: DataStream[(String, Long, Int)] = textDstream.map { text =>
      val arr: Array[String] = text.split(" ")
      (arr(0), arr(1).toLong, 1)
    }

   // val textWithEventTimeDstream: DataStream[(String, Long, Int)] = textTupleDstream.assignAscendingTimestamps(_._2).setParallelism(1)
    val textWithEventTimeDstream: DataStream[(String, Long, Int)] = textTupleDstream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Int)](Time.milliseconds(1000)) {
      override def extractTimestamp(element: (String, Long, Int)): Long = {
        element._2
      }
    }).setParallelism(1)


    //滚动
    //val windowDStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textWithEventTimeDstream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(10)))//左闭右开

    //滑动
    //val windowDStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textWithEventTimeDstream.keyBy(0).window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))//左闭右开
    //session
    val windowDStream: WindowedStream[(String, Long, Int), Tuple, TimeWindow] = textWithEventTimeDstream.keyBy(0).window(EventTimeSessionWindows.withGap(Time.seconds(5)))//左闭右开


//    val groupDstream: DataStream[mutable.HashSet[Long]] = windowDStream.fold(new mutable.HashSet[Long]()) { case (set, (key, ts, count)) =>
//      set += ts
//    }

    windowDStream.reduce((text1,text2)=>
      (  text1._1,0L,text1._3+text2._3)
    )  .map(_._3).print("windows:::").setParallelism(1)


    //groupDstream.print("window=============>")

    env.execute()

  }

}
