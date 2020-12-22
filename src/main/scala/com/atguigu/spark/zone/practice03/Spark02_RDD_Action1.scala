package com.atguigu.spark.zone.practice03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Action1 {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Spark - RDD - 行动算子
        val rdd: RDD[Int] = sc.makeRDD(
            List(3, 1, 4, 2)
        )

        // TODO
        val ints: Array[Int] = rdd.takeOrdered(2)
        println(ints.mkString(","))


        sc.stop()
    }

}
