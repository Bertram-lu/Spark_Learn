package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object SparkStreaming02_DStream_Queue1 {
    def main(args: Array[String]): Unit = {

        //TODO 配置对象
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        //TODO 环境对象
        val ssc = new StreamingContext(conf,Seconds(5))

        //TODO 数据处理
        val que = new mutable.Queue[RDD[String]]()
        val queueDS: InputDStream[String] = ssc.queueStream(que)

        queueDS.print()

        //TODO 关闭连接环境
        ssc.start()
        for (i <- 1 to 5) {
            val rdd: RDD[String] = ssc.sparkContext.makeRDD(List("a","b","c"))
            que += rdd
            Thread.sleep(2000)
        }

        ssc.awaitTermination()

    }

}
