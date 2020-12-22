package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Test2 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val logRDD: RDD[String] = sc.textFile("Spark/input/apache.log")

        val timeRDD: RDD[Char] = logRDD.map(
            datas => {
                datas.split(" ")
                datas(4)
            }
        )
        timeRDD


        sc.stop()
    }

}
