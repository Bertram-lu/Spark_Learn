package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_WordCount3 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local")
        val sc = new SparkContext(conf)

        val fileRDD: RDD[String] = sc.textFile("Spark/input")
        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
        val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))
        val value: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)
        val result: Array[(String, Int)] = value.collect()
        result.foreach(println)

        sc.stop()
    }

}
