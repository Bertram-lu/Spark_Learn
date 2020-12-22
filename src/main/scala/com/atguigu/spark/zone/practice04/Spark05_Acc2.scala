package com.atguigu.spark.zone.practice04


import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Acc2 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("acc").setMaster("local[*]")
        val sc: SparkContext = new SparkContext(conf)

        // TODO Spark - 自定义累加器 - wordcount
        //累加器可以不使用shuffle就完成数据的聚合功能
        val rdd: RDD[String] = sc.makeRDD(List(
            "hello world", "hello spark", "hello"
        ))

        // TODO 1. 创建累加器
        val acc = new WordCountAccumulator

        // TODO 2. 向Spark注册累加器
        sc.register(acc,"wordcount")

        // TODO 3. 使用累加器
        rdd.foreach(
            words => {
                val ws: Array[String] = words.split(" ")
                ws.foreach(
                    word => {
                        acc.add(word)
                    }
                )
            }
        )

        println(acc.value)

        sc.stop()
    }

    class WordCountAccumulator extends AccumulatorV2[String,mutable.Map[String,Int]] {
        var innerMap = mutable.Map[String,Int]()

        // TODO 累加器是否初始化
        override def isZero: Boolean = innerMap.isEmpty

        // TODO 复制累加器
        override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
            new WordCountAccumulator
        }

        // TODO 重置累加器
        override def reset(): Unit = {
            innerMap.clear()
        }

        // TODO 累加数据
        override def add(word: String): Unit = {
            val cnt = innerMap.getOrElse(word,0)
            innerMap.update(word,cnt + 1)
        }

        // TODO 合并累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
            val map1: mutable.Map[String, Int] = this.innerMap
            val map2: mutable.Map[String, Int] = other.value

            innerMap = map1.foldLeft(map2)(
                (map,kv) => {
                    val k: String = kv._1
                    val v: Int = kv._2
                    map(k) = map.getOrElse(k,0) + v
                    map
                }
            )
        }

        override def value: mutable.Map[String, Int] = innerMap
    }

}
