package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark26_RDD_Test1 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val logRDD: RDD[String] = sc.textFile("Spark/input/apache.log")

        val timeRDD: RDD[String] = logRDD.map(
            log => {
                val dates: Array[String] = log.split(" ")
                dates(3)
            }
        )
        val filterRDD: RDD[String] = timeRDD.filter(
            time => {
                val ymd: String = time.substring(0, 10)
                ymd == "17/05/2015"
            }
        )
        filterRDD.collect().foreach(println)

        sc.stop()
    }

}
