package com.atguigu.flink1205

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._


object DateSetWcApp {


  def main(args: Array[String]): Unit = {
    // 1 env  //2 source   //3 transform   //4 sink
    val tool: ParameterTool = ParameterTool.fromArgs(args)
    val inputPath: String = tool.get("input")
    val outputPath: String = tool.get("output")

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val txtDataSet: DataSet[String] = env.readTextFile(inputPath)

    val aggSet: AggregateDataSet[(String, Int)] = txtDataSet.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)

    aggSet.writeAsCsv(outputPath)

    env.execute()

  }

}
