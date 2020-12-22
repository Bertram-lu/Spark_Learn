package com.atguigu.spark.zone.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming04_DStream_DIY {
    def main(args: Array[String]): Unit = {

        //TODO 配置对象
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordcount")

        //TODO 环境对象
        val ssc = new StreamingContext(conf,Seconds(5))

        //TODO 数据处理
        val myDS = ssc.receiverStream(new MyReceiver("hadoop102",9999))
        val wordDS: DStream[String] = myDS.flatMap(_.split(" "))
        val wordToOneDS: DStream[(String, Int)] = wordDS.map((_,1))
        val wordToCountDS: DStream[(String, Int)] = wordToOneDS.reduceByKey(_+_)
        wordToCountDS.print()

        //TODO 关闭连接环境
        ssc.start()
        ssc.awaitTermination()

    }
    class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){
        private var socket: Socket = _

        def receive() : Unit = {
            val reader = new BufferedReader(
                new InputStreamReader(
                    socket.getInputStream,
                    "UTF-8"
                )
            )

            var s : String = null

            while ( (s = reader.readLine()) != null ) {
                // 采集到数据后，进行封装(存储)
                if ( s != "-END-" ) {
                    store(s)
                } else {
                    // stop
                    // close
                    // 重启
                    //restart("")
                }

            }

        }

        override def onStart(): Unit = {

            socket = new Socket(host,port)

            // Start the thread that receives data over a connection
            new Thread("Socket Receiver") {
                setDaemon(true)
                override def run() { receive() }
            }.start()
        }

        override def onStop(): Unit = {

            if (socket != null) {
                socket.close()
            }
        }
    }
}
