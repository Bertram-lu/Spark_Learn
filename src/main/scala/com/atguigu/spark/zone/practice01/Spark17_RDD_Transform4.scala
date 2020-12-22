package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Transform4 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val list = List(2,5,6,7)
        val rdd: RDD[Int] = sc.makeRDD(list,2)
        val indexRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex(
            (pindex, datas) => {
                datas.map(
                    (pindex, _)
                )
            }
        )
        indexRDD.collect().foreach(println)

        sc.stop()
    }

}
