package com.atguigu.flink1205.util

import java.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.flink1205.bean.Startuplog
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentType



object MyEsUtil {


  val httpHosts = new  util.ArrayList[HttpHost]
  httpHosts.add(new HttpHost("hadoop1",9200,"http"))
  httpHosts.add(new HttpHost("hadoop2",9200,"http"))
  httpHosts.add(new HttpHost("hadoop3",9200,"http"))


  def getEsSink(indexName:String): ElasticsearchSink[Startuplog] ={

    val esSinkBuilder: ElasticsearchSink.Builder[Startuplog] = new ElasticsearchSink.Builder[Startuplog] (httpHosts,new MyElasticSearchFunction(indexName))
    esSinkBuilder.setBulkFlushMaxActions(20)
    val esSink: ElasticsearchSink[Startuplog] = esSinkBuilder.build()
    esSink
  }

  class MyElasticSearchFunction(indexName:String) extends  ElasticsearchSinkFunction[Startuplog] {
    override def process(element: Startuplog, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {

      implicit val formats = org.json4s.DefaultFormats
      val jsonstr: String = org.json4s.native.Serialization.write(element)
      val indexRequest: IndexRequest = Requests.indexRequest().index(indexName).`type`("_doc").source(jsonstr,XContentType.JSON)
      indexer.add(indexRequest)
    }



  }

}
