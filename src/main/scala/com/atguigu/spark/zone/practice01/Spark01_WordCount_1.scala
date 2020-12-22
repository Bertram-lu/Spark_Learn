package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount_1 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local")
        val sc = new SparkContext(conf)

        val fileRDD: RDD[String] = sc.textFile("Spark/input")

        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

        val wordOneRDD: RDD[(String, Int)] = wordRDD.map((_,1))

        val wordIterRDD: RDD[(String, Iterable[(String, Int)])] = wordOneRDD.groupBy(_._1)

        val wordCountRDD: RDD[(String, Int)] = wordIterRDD.map {
            case (word, list) => {
                (word, list.size)
            }
        }
        val result: Array[(String, Int)] = wordCountRDD.collect()
        result.foreach(println)

        sc.stop()

    }

}
