package com.atguigu.spark.zone.practice04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Acc1 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("acc").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        // TODO Spark - 累加器

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4,5))

        val sum = sc.longAccumulator("sum")

        rdd.foreach(
            num => {
                sum.add(num)
                println("executor:sum =" + sum.value)
            }
        )

        println("sum = " + sum.value)

        sc.stop()
    }

}
