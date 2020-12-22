package com.atguigu.spark.zone.streaming


import com.google.common.hash.HashingOutputStream
import kafka.common.{OffsetOutOfRangeException, TopicAndPartition}
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming08_DStream_Kafka_Direc2 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(conf,Seconds(5))

        // TODO 数据处理
        // 使用0.8版本的kafka - Direct方式 - 手动维护Offset
        // 所谓的手动维护，其实就是开发人员自己获取偏移量，并进行保存处理。
        // 通过保存的偏移量，可以动态获取kafka中指定位置的数据
        // offset会保存到kakfa集群的系统主题中__consumer_offsets
        val kafkaMap: Map[String, String] = Map(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
            ConsumerConfig.GROUP_ID_CONFIG -> "sparkstreaming"
        )

        val fromOffset: Map[TopicAndPartition, Long] = Map(
            (TopicAndPartition("sparkstreaming", 0), 0L),
            (TopicAndPartition("sparkstreaming", 1), 1L),
            (TopicAndPartition("sparkstreaming", 2), 2L)
        )

        val kafkaDS: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
            ssc,
            kafkaMap,
            fromOffset,
            (m: MessageAndMetadata[String, String]) => m.message()
        )

        var offsetRanges = Array.empty[OffsetRange]

        kafkaDS.transform(rdd => {
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
            rdd
        }).foreachRDD(rdd=>{
            for (o <- offsetRanges){
                println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
            }
            rdd.foreach(println)
         })

        ssc.start()
        ssc.awaitTermination()
    }

}
