package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark15_RDD_Transform3 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val list = List(1,2,3,4)

        val rdd: RDD[Int] = sc.makeRDD(list,2)
        val numRDD: RDD[Int] = rdd.mapPartitions(
            datas => {
                datas.filter(_ % 2 == 0)
            }
        )
        numRDD.saveAsTextFile("Spark/output")
        sc.stop()
    }

}
