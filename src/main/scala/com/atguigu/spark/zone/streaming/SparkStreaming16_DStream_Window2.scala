package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming16_DStream_Window2 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(conf,Seconds(3))

        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
        // TODO 窗口范围必须为采集周期的整数倍
        // TODO 滑动步长必须为采集周期的整数倍
        val windowDS: DStream[String] = socketDS.window(
            Seconds(6), Seconds(3)
        )
        windowDS
            .flatMap(_.split(" "))
            .map((_,1))
            .reduceByKey(_+_)
            .print()

        ssc.start()
        ssc.awaitTermination()

    }

}
