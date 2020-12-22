package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object Spark36_RDD_Transform17 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        val rdd: RDD[(String, String)] = sc.makeRDD(
            List(
                ("rog", "qiangshen"),
                ("lianxiang", "zhengjiuzhge"),
                ("rog", "bingren"),
                ("huipu", "anyingjingling")
            ),
            2
        )
        //val rdd1: RDD[(String, String)] = rdd.partitionBy(new HashPartitioner(2))
        val rdd1: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner(2))

        val rdd2: RDD[(Int, (String, String))] = rdd1.mapPartitionsWithIndex(
            (index, datas) => {
                datas.map(
                    d => {
                        (index, d)
                    }
                )
            }
        )
        rdd2.collect().foreach(println)
        sc.stop()
    }

    //自定义分区器
    class MyPartitioner (num : Int) extends Partitioner {
        def numPartitions: Int = num

        def getPartition(key: Any): Int = {
            if ( key.isInstanceOf[String] ) {
                val keystring: String = key.asInstanceOf[String]
                if ( keystring == "rog") {
                    0
                } else {
                    1
                }
            } else {
                1
            }
        }
    }


}
