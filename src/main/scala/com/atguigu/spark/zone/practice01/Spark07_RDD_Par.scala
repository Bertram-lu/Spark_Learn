package com.atguigu.spark.zone.practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Par {
    def main(args: Array[String]): Unit = {

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        // TODO 分区 => 并行度 => 并行计算能力

        // 默认分区数量源码：
        // scheduler.conf.getInt("spark.default.parallelism", totalCores)
        // 从配置信息中获取配置项目，如果获取到，直接使用
        // 如果获取不到，使用默认值，本地环境，默认值为cpu核数

        val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
        //如果是本地环境，会采用local的设定参数作为并行度设置
        //获取分区数量
        //println(rdd.partitions.length)

        //讲数据以分区的方式保存
        //rdd.saveAsTextFile("spark/output")

        // TODO 改变默认分区 => 设定分区数量
        val rdd1: RDD[Int] = sc.makeRDD(List(1,2,3,4),4)
        rdd1.saveAsTextFile("spark/output")
        sc.stop()
    }

}
