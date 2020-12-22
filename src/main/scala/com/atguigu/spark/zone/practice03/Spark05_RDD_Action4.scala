package com.atguigu.spark.zone.practice03

import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Action4 {

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Spark - RDD - 行动算子
        val rdd = sc.makeRDD(List("Hello","World","Hello","Scala"),1)

        rdd.saveAsTextFile("Spark/output1")
        rdd.saveAsObjectFile("Spark/output2")
        rdd.map((_,1)).saveAsSequenceFile("Spark/output3")

        sc.stop()
    }

}
