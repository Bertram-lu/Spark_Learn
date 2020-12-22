package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming11_DStream_Transform {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(conf,Seconds(5))
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

        // TODO transform可以获取底层的RDD进行处理
        // TODO transform可以周期性的执行driver的代码逻辑
        val newDS: DStream[String] = socketDS.transform(
            rdd => {
                println(Thread.currentThread().getName)
                rdd.map(
                    dataString => {
                        "string : " + dataString
                    }
                )
            }
        )
        newDS.print()



        ssc.start()
        ssc.awaitTermination()
    }
}
