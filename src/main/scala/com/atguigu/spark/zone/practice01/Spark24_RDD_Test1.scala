package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Test1 {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO : 将List("Hello", "hive", "hbase", "Hadoop")根据单词首写字母进行分组

        val wordRDD: RDD[String] = sc.makeRDD(
            List("Hello", "hive", "hbase", "Hadoop"),2
        )
        val groupRDD: RDD[(Char, Iterable[String])] = wordRDD.groupBy(
            word => {
                //word.substring(0,1)
                //word.charAt(0)
                word(0)
            }
        )
        //groupRDD.collect().foreach(println)
        groupRDD.saveAsTextFile("Spark/output")

        sc.stop()
    }

}
