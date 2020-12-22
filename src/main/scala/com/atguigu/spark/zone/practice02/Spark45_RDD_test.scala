package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark45_RDD_test {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        //TODO Scala - 转换算子 - sortByKey

        sc.stop()


    }
    class User extends Ordered[User] {
        override def compare(that: User): Int = {
            1
        }
    }
}
