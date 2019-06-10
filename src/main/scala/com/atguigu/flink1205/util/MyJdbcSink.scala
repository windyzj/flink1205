package com.atguigu.flink1205.util

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

class MyJdbcSink(sql:String)  extends  RichSinkFunction[Array[Any]]{

  val driver="com.mysql.jdbc.Driver"

  val url="jdbc:mysql://hadoop2:3306/gmall1205?useSSL=false"

  val username="root"

  val password="123123"

  var connection:Connection=null



  //1+N+1
 //创建连接
  override def open(parameters: Configuration): Unit ={
      Class.forName(driver)
      connection  = DriverManager.getConnection(url,username,password)
  }

  override def invoke(valueArr: Array[Any]): Unit = {
    val preparedStatement: PreparedStatement = connection.prepareStatement(sql)

    for ( i <- 0 to valueArr.size-1  ) {
      preparedStatement.setObject(i+1,valueArr(i))

    }
    preparedStatement.executeUpdate()


  }


  override def close(): Unit = {
    if(connection!=null){
      connection.close()
    }

  }

}
