package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming01_WordCount1 {
    def main(args: Array[String]): Unit = {

        //TODO 配置对象
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        //TODO 环境对象
        val ssc = new StreamingContext(conf,Seconds(5))

        //TODO 数据处理
        //TODO 从数据源采集数据
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

        //TODO 将采集的数据进行wordcount的处理
        val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))
        val wordToCountDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)

        wordToCountDS.print()

        //TODO 关闭连接环境
        ssc.start()

        ssc.awaitTermination()
    }

}
