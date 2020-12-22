package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount1 {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local")
        val sc = new SparkContext(conf)
        sc
            .textFile("Spark/input")
            .flatMap(_.split(" "))
            .map((_,1))
            .groupBy(_._1)
            .map {
                case (word, list) => {
                    (word, list.size)
                }
            }
            .collect()
            .foreach(println)
        sc.stop()
    }

}
