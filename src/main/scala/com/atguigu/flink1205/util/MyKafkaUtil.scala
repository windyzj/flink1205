package com.atguigu.flink1205.util

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object MyKafkaUtil {

  val prop = new Properties()

  prop.setProperty("bootstrap.servers", "hadoop1:9092")
  prop.setProperty("group.id", "gmall")


 def  getKafkaSource(topic:String): FlinkKafkaConsumer011[String]={
   val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),prop)
   kafkaConsumer
  }

  def getKafkaSink(topic:String): FlinkKafkaProducer011[String] ={
      new FlinkKafkaProducer011[String]("hadoop1:9092",topic,new SimpleStringSchema())
  }

}


