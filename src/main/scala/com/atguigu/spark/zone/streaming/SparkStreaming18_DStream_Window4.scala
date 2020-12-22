package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming18_DStream_Window4 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(conf,Seconds(3))
        ssc.checkpoint("scp")

        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

        val countDS: DStream[Long] = socketDS.countByWindow(Seconds(6),Seconds(3))

        countDS.print()

        ssc.start()
        ssc.awaitTermination()

    }

}
