package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable


object SparkStreaming05_DStream_Kafka {
    def main(args: Array[String]): Unit = {

        //TODO 配置对象
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        //TODO 环境对象
        val ssc = new StreamingContext(conf,Seconds(5))

        //TODO 数据处理
        val kafkaDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(
            ssc,
            "hadoop102:2181,hadoop103:2181,hadoop104:2181",
            "sparkstreaming",
            Map[String, Int]("sparkstreaming" -> 3)
        )

        kafkaDS.map(_._2).print()

        //TODO 关闭连接环境
        ssc.start()
        ssc.awaitTermination()

    }

}
