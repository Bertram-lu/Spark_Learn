package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming10_DStream_WordCount {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(conf,Seconds(5))
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

        val resultRDD: DStream[(String, Int)] = socketDS.transform(
            rdd => {
                val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
                val wordToOneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
                val wordToCountRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_ + _)
                wordToCountRDD
            }
        )
        resultRDD.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
