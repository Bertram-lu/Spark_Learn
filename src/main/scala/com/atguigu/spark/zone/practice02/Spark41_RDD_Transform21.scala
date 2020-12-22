package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark41_RDD_Transform21 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        //TODO Scala - 转换算子 - foldByKey
        //如果aggregateByKey分区内计算规则和分区间计算规则相同
        //那么可以采用其他算子来代替

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("c", 3),
            ("b", 4), ("c", 5), ("c", 6)
        ), 2)

        val resultRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_+_)

        resultRDD.collect().foreach(println)

        sc.stop()

    }
}
