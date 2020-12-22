package com.atguigu.spark.zone.practice04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Serial {

    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd")

        val sc = new SparkContext(conf)

        val rdd: RDD[String] = sc.makeRDD(Array("hello world", "hello spark", "hive", "atguigu"))

        val search = new Search("hello")

        //val matchRDD: RDD[String] = search.getMatch1(rdd)
        val matchRDD: RDD[String] = search.getMatch2(rdd)

        matchRDD.foreach(println)

        sc.stop()
    }

    class Search(query:String) extends Serializable {

        def isMatch(s:String) : Boolean = {
            s.contains(query)
        }

        //函数序列化案例
        def getMatch1(rdd:RDD[String]):RDD[String] = {
            rdd.filter(this.isMatch)
            rdd.filter(isMatch)
        }

        //属性序列化案例
        def getMatch2(rdd:RDD[String]):RDD[String] = {
            //rdd.filter(x => x.contains(this.query))
            //rdd.filter(x => x.contains(query))
            val q = query
            rdd.filter(x =>x.contains(q))
        }
    }

}
