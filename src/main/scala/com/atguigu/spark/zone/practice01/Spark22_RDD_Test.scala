package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_RDD_Test {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)
        val rdd: RDD[Int] = sc.makeRDD(List(1,3,6,9),2)

        val glomRDD: RDD[Array[Int]] = rdd.glom()
        val maxRDD: RDD[Int] = glomRDD.map(
            array => array.max
        )
        val ints: Array[Int] = maxRDD.collect()
        println(ints.sum)

        sc.stop()
    }

}
