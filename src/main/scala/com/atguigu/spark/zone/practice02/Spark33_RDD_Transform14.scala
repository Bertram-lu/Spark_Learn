package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark33_RDD_Transform14 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Spark - 转换算子
        val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        val rdd2: RDD[Int] = sc.makeRDD(List(3,4,5,6))
        val rdd6: RDD[String] = sc.makeRDD(List("3","4","5","6"))

        //交集，并集，差集调用时所传递的RDD数据类型要和当前的RDD类型相同

        // 并集
        val rdd3: RDD[Int] = rdd1.union(rdd2)
        //val rdd7: RDD[Int] = rdd1.union(rdd6)

        // 交集
        val rdd4: RDD[Int] = rdd1.intersection(rdd2)

        // 差集
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)

        println(rdd3.collect().mkString(","))
        println(rdd4.collect().mkString(","))
        println(rdd5.collect().mkString(","))

        sc.stop()
    }
}
