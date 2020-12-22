package com.atguigu.spark.zone.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {

        // TODO 10种实现wordcount的方法

        val conf = new SparkConf().setMaster("local[*]").setAppName("rdd")
        val sc = new SparkContext(conf)

        val rdd: RDD[String] = sc.makeRDD(List("hello","word","hello","spaek"))

        // TODO 第一种
        val oneWC: collection.Map[String, Long] = rdd.map((_,1)).countByKey()
        println(oneWC)

        // TODO 第二种
        val twoWC: collection.Map[String, Long] = rdd.countByValue()
        println(twoWC)

        // TODO 第三种
        val threeWC: RDD[(String, Int)] = rdd.map((_,1)).groupByKey().mapValues(_.sum)
        println(threeWC.collect().mkString(","))

        // TODO 第四种
        val fourWC: RDD[(String, Int)] = rdd.map((_,1)).aggregateByKey(0)(_+_,_+_)
        println(fourWC.collect().mkString(","))

        // TODO 第五种
        val fiveWC: RDD[(String, Int)] = rdd.map((_,1)).foldByKey(0)(_+_)
        println(fiveWC.collect().mkString(","))

        // TODO 第六种
        val sixWC: RDD[(String, Int)] = rdd.map((_, 1)).combineByKey(
            (num: Int) => num,
            (x: Int, y: Int) => {
                x + y
            },
            (x: Int, y: Int) => {
                x + y
            }
        )
        println(sixWC.collect().mkString(","))

        // TODO 第七种
        val sevenWC: RDD[(String, Int)] = rdd.map((_,1)).reduceByKey(_+_)
        println(sevenWC.collect().mkString(","))

        // TODO 第八种
        val eightWC: RDD[(String, Int)] = rdd.map((_,1)).groupBy(_._1).map{case (word , list) => (word,list.size)}
        println(eightWC.collect().mkString(","))

    }

}
