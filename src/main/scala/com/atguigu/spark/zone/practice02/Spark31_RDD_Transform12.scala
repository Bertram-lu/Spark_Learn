package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark31_RDD_Transform12 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Scala - 转换算子 - coalesce - 改变分区

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6),3)

        val rdd1: RDD[Int] = rdd.repartition(4)

        val rdd3: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex(
            (index, datas) => {
                datas.map(
                    d => {
                        (index, d)
                    }
                )
            }
        )
        rdd3.collect().foreach(println)
        sc.stop()
    }

}
