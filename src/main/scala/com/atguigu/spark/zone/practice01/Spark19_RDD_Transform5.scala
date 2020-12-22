package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Transform5 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)
        val rdd: RDD[List[Int]] = sc.makeRDD(List(
            List(2, 4), List(3, 6), List(4, 8)
        ),2)
        val rdd1: RDD[Int] = rdd.flatMap(
            list => {
                list
            }
        )
        println(rdd1.collect().mkString(","))
    }

}
