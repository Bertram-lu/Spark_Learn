package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Test_1 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val list = List(2,5,7,9)

        val rdd: RDD[Int] = sc.makeRDD(list,2)

        val maxRDD: RDD[Int] = rdd.mapPartitions(
            datas => {
                List(datas.max).iterator
            }
        )
        println(maxRDD.collect().mkString(","))

        sc.stop()
    }

}
