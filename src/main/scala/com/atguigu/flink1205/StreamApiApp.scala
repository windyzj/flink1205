package com.atguigu.flink1205

import com.alibaba.fastjson.JSON
import com.atguigu.flink1205.bean.Startuplog
import com.atguigu.flink1205.util.{MyEsUtil, MyJdbcSink, MyKafkaUtil, RedisUtil}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, KeyedStream, SplitStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.redis.RedisSink

object StreamApiApp {

  def main(args: Array[String]): Unit = {
      val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(8)
      val kafaSource: FlinkKafkaConsumer011[String] = MyKafkaUtil.getKafkaSource("GMALL_STARTUP")
      val dstream: DataStream[String] = env.addSource(kafaSource)

   // dstream.print(":::")

    val startuplogDstream: DataStream[Startuplog] = dstream.map{ jsonstr=>JSON.parseObject(jsonstr,classOf[Startuplog])}


    //根据标签进行切分
//    val splitStream: SplitStream[Startuplog] = startuplogDstream.split { startuplog =>
//      var flag: List[String] = null;
//      if (startuplog.ch == "appstore") {
//        flag = List("apple", "usa")
//      } else if (startuplog.ch == "huawei") {
//        flag = List("android", "china")
//      } else {
//        flag = List("android", "other")
//      }
//      flag
//    }
//    val appleStream: DataStream[Startuplog] = splitStream.select("apple" )
//    val otherStream: DataStream[Startuplog] = splitStream.select("other")

   // appleStream.print("apple")
  //  otherStream.print("other")

//    val connStream: ConnectedStreams[Startuplog, Startuplog] = appleStream.connect(otherStream)
//    val allDstream: DataStream[String] = connStream.map(
//      (startuplog1: Startuplog) => startuplog1.ch,
//      (startuplog2: Startuplog) => startuplog2.ch
//    )
//    allDstream.print("ALL")

//    val unionDstream: DataStream[Startuplog] = appleStream.union(otherStream)
//    unionDstream.print("UNION")


//   val kafkaSink: FlinkKafkaProducer011[String] = MyKafkaUtil.getKafkaSink("topic_apple")
//    appleStream.map( startuplog=> startuplog.ch ).addSink(kafkaSink)


//    //求各个渠道的累计个数
//        val chKeyedStream: KeyedStream[(String, Int), Tuple] = startuplogDstream.map(startuplog => (startuplog.ch, 1)).keyBy(0)
//        val chSumDstream: DataStream[(String, Int)] = chKeyedStream.reduce{ (ch1, ch2)=> (ch1._1,ch1._2+ch2._2)}
//
//        chSumDstream.print("zhangchen").setParallelism(1)
//       //把结果存入redis    hset  key: channel_sum    field:  channel    value : count
//
//    val redisSink: RedisSink[(String, Int)] = RedisUtil.getRedisSink()
//    chSumDstream.addSink(redisSink)
//
////    val esSink: ElasticsearchSink[Startuplog] = MyEsUtil.getEsSink("gmall1205_startup_flink")
////    appleStream.addSink(esSink)
////
//    val jdbcDStream: DataStream[Array[Any]] = appleStream.map(startuplog=>  Array(startuplog.mid,startuplog.ch,startuplog.area,startuplog.vs,startuplog.uid))
//    val sink:MyJdbcSink = new MyJdbcSink("insert into z_startup values(?,?,?,?,?)")
//    jdbcDStream.addSink(sink)


    //求每5秒钟统计，该批次各个渠道的累计个数
    val chKeyedStream: KeyedStream[(String, Int), Tuple] = startuplogDstream.map(startuplog => (startuplog.ch, 1)).keyBy(0)
   // val windowsStream: WindowedStream[(String, Int), Tuple, TimeWindow] = chKeyedStream.timeWindow(Time.seconds(20),Time.seconds(5))
   val windowsStream: WindowedStream[(String, Int), Tuple, GlobalWindow] = chKeyedStream.countWindow(100L,10L)
    val chSumDstream: DataStream[(String, Int)] = windowsStream.reduce{ (ch1, ch2)=> (ch1._1,ch1._2+ch2._2)}

    chSumDstream.print("windows::::::").setParallelism(1)

    env.execute()
  }



}
