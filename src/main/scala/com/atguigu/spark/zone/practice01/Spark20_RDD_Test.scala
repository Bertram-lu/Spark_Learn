package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Test {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val rdd = sc.makeRDD( List(
            List(1,2),
            3,
            List(4,5)), 1 )

        val rdd1: RDD[Any] = rdd.flatMap (
            datas => {
                datas match {
                    case datas: List[_] => datas
                    case d => List(d)
                }
            }
        )
        println(rdd1.collect().mkString(","))
    }


}
