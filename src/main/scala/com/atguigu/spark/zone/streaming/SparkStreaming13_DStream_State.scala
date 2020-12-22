package com.atguigu.spark.zone.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming13_DStream_State {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        ssc.checkpoint("scp")
        val socketDS: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)

        val wordDS: DStream[String] = socketDS.flatMap(_.split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))
        // TODO 使用有状态操作updateStateByKey保存数据
        // SparkStreaming的状态保存依赖的是checkpoint,所以需要设定相关路径
        val wordToCountDS: DStream[(String, Long)] = wordToOneDS.updateStateByKey[Long](
            (seq: Seq[Int], buffer: Option[Long]) => {
                val newBufferValue: Long = buffer.getOrElse(0L) + seq.sum
                Option(newBufferValue)
            }
        )
        wordToCountDS.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
