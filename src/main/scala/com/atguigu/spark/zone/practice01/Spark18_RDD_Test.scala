package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Test {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val rdd: RDD[Int] = sc.makeRDD(List(5,6,7,8),3)
        val indexRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
            (index, datas) => {
                if (index == 1) {
                    datas
                } else {
                    Nil.iterator
                }
            }
        )
        println(indexRDD.collect().mkString(","))
    }

}
