package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark44_RDD_Transform23 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        //TODO Scala - 转换算子 - sortByKey
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("d", 3), ("b", 4), ("c", 2)
        ))
        val rdd1: RDD[(String, Int)] = rdd.sortByKey(true)
        val rdd2: RDD[(String, Int)] = rdd.sortByKey(false)

        //rdd1.collect().foreach(println)
        rdd2.collect().foreach(println)

        sc.stop()


    }
}
