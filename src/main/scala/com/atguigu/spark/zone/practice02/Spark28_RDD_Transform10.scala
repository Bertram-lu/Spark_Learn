package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark28_RDD_Transform10 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Scala - 转换算子 - distinct - 数据去重

        // WordCount
        // ("Hello", "Hello") => ("Hello", 2) => ("Hello")
        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5,6,5,4,3,1,2))

        // distinct实现源码：
        // map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
        val distinctRDD: RDD[Int] = rdd.distinct()
        // 自定义去重
        //val rdd1: RDD[Int] = rdd.map((_,1)).reduceByKey(_+_).map(_._1)

        println(distinctRDD.collect().mkString(","))

    }

}
