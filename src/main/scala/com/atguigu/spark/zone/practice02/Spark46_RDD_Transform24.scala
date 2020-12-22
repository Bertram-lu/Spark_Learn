package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark46_RDD_Transform24 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        //TODO Scala - 转换算子 - join
        val rdd1: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("b", 2) , ("a",3)
            )
        )
        val rdd2: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("b", 4), ("a", 5)
            )
        )

        val result: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        val zipRDD: RDD[((String, Int), (String, Int))] = rdd1.zip(rdd2)

        result.collect().foreach(println)

        //zipRDD.collect().foreach(println)

        sc.stop()


    }
}
