package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

object SparkStreaming01_WordCount {
    def main(args: Array[String]): Unit = {

        //TODO 配置对象
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        //TODO 环境对象
        //val ssc = new StreamingContext(conf,Duration(1000*5))
        //创建对象的第二个参数表示数据的采集周期
        val ssc = new StreamingContext(conf,Seconds(5))

        //TODO 数据处理
        //TODO 从数据源采集数据
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

        //TODO 将采集的数据进行wordcount的处理
        val wordDs: DStream[String] = socketDS.flatMap(_.split(" "))

        val wordToOneDs: DStream[(String, Int)] = wordDs.map((_,1))

        val wordToCountDS: DStream[(String, Int)] = wordToOneDs.reduceByKey(_+_)

        wordToCountDS.print()

        //TODO 关闭连接环境
        //关闭sparkstreaming是可以的，但是一般在程序升级和出现故障时
        //所以在一般情况下是不能关闭的，并且不能让driver程序结束，需要让driver程序等待
        //ssc.stop()

        //启动数据采集器，开始采集数据
        //启动数据处理环境
        ssc.start()

        //让driver程序等待数据处理的停止或异常时，才会继续执行
        ssc.awaitTermination()
    }

}
