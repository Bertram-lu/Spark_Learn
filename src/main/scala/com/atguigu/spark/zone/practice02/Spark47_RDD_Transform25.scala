package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark47_RDD_Transform25 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        //TODO Scala - 转换算子 - OuterJoin
        val rdd1: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("b", 2) , ("c",3)
            )
        )
        val rdd2: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 4), ("b", 5)
            )
        )

        //val result: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)

        val result1: RDD[(String, (Option[Int], Int))] = rdd2.rightOuterJoin(rdd1)

        result1.collect().foreach(println)

        sc.stop()


    }
}
