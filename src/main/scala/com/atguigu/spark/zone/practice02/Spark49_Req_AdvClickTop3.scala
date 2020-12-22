package com.atguigu.spark.zone.practice02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark49_Req_AdvClickTop3 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        val fileRDD: RDD[String] = sc.textFile("Spark/input/agent.log")

        val mapRdd: RDD[(String, Int)] = fileRDD.map(
            line => {
                val datas: Array[String] = line.split(" ")
                (datas(1) + "-" + datas(4), 1)
            }
        )

        val reduceRDD: RDD[(String, Int)] = mapRdd.reduceByKey(_+_)

        val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map {
            case (k, cnt) => {
                val ks: Array[String] = k.split("-")
                (ks(0), (ks(1), cnt))
            }
        }

        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

        val top3RDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                iter.toList.sortWith(
                    (left, right) => {
                        left._2 > right._2
                    }
                ).take(3)
            }
        )

        top3RDD.collect().foreach(println)

        sc.stop()


    }
}
