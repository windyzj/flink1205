package com.atguigu.flink1205.util

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.flink1205.bean.Startuplog
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

object MyEsUtil2 {

  val httpHostList = new util.ArrayList[HttpHost]
  httpHostList.add(new HttpHost("hadoop1", 9200))
  httpHostList.add(new HttpHost("hadoop2", 9200))
  httpHostList.add(new HttpHost("hadoop3", 9200))

  def getEsSink(indexName: String): ElasticsearchSink[Startuplog] = {

    val esFunc = new ElasticsearchSinkFunction[Startuplog] {
      override def process(element: Startuplog, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

         JSON.parseObject(element)
         //   val jsonStr: String = JSON.toJSONString(element,SerializerFeature.)

//        val map: util.Map[String,String] = JSON.parseObject(str,classOf[util.Map[String,String]])
        println(element)


        val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(str)
        indexer.add(indexRequest)
      }
    }


    val esSinkBuilder = new ElasticsearchSink.Builder[Startuplog](httpHostList, esFunc)

    esSinkBuilder.setBulkFlushMaxActions(10)

    esSinkBuilder.build()

  }
}