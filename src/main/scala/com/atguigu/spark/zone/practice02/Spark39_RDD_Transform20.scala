package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark39_RDD_Transform20 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        //TODO Scala - 转换算子 - aggregateByKey

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 2), ("c", 3),
            ("b", 4), ("c", 5), ("c", 6)
        ), 2)

        // 0:("a",1),("a",2),("c",3) => (a,10)(c,10)
        //                                         => (a,10)(b,10)(c,20)
        // 1:("b",4),("c",5),("c",6) => (b,10)(c,10)

        //TODO 分区内相同key的求最大值，然后分区间相加
        //aggregateByKey算子是函数柯里化，存在两个参数列表
        //1.第一个参数列表中的参数表示初始值
        //2.第二个参数列表中含有两个参数
        //    2.1 第一个参数表示分区内的计算规则
        //    2.2 第二个参数表示分区间的计算规则

        val resultRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        )

        /*//分区内聚合
        List(1,2,3,4).reduce()

        //分区内和分区间聚合
        List(1,2,3,4).fold(0)()*/

        resultRDD.collect().foreach(println)

        sc.stop()

    }
}
