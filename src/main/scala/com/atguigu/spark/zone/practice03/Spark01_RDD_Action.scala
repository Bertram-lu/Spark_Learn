package com.atguigu.spark.zone.practice03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Action {

    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO Spark - RDD - 行动算子
        // 这里的行动的概念指的是，让当前应用程序开始执行
        val rdd: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4)
        )

        // TODO reduce
        // sc.runJob
        val i: Int = rdd.reduce(_+_)
        println(" i = " + i)

        // TODO collect
        val ints: Array[Int] = rdd.collect()
        println(ints.mkString(","))

        // TODO Count
        val cnt: Long = rdd.count()
        println(cnt)

        // TODO first
        val first: Int = rdd.first()
        println(first)

        // TODO take
        val top3: Array[Int] = rdd.take(3)
        top3.foreach(println)

        sc.stop()
    }

}
