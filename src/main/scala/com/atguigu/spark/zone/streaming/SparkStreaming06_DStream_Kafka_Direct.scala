package com.atguigu.spark.zone.streaming


import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_DStream_Kafka_Direct {
    def main(args: Array[String]): Unit = {

        //TODO 配置对象
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        //TODO 环境对象
        val ssc = new StreamingContext(conf,Seconds(5))
        ssc.checkpoint("scp")

        //TODO 数据处理
        val kafkaParamMap: Map[String, String] = Map(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "sparkstreaming"
        )

        val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
            ssc,
            kafkaParamMap,
            Set("sparkstreaming")
        )

        kafkaDS.map(_._2).print()

        //TODO 关闭连接环境
        ssc.start()
        ssc.awaitTermination()

    }

}
