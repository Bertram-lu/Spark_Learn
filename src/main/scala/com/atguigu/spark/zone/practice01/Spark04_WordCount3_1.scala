package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_WordCount3_1 {
    def main(args: Array[String]): Unit = {
        //获取Spark的连接对象
        val conf: SparkConf = new SparkConf().setAppName("wordcount").setMaster("local")
        val context = new SparkContext(conf)

        //获取文件资源
        val fileRDD: RDD[String] = context.textFile("Spark/input")

        //按照字母一个一个切分
        val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

        //按照需求改变其数据结构
        val mapRDD: RDD[(String, Int)] = wordRDD.map((_,1))

        //分组然后聚合
        val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_+_)

        //将聚合结果展示在控制台
        val result: Array[(String, Int)] = resultRDD.collect()
        result.foreach(println)
    }

}
