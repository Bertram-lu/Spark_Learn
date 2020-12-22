package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL06_UDAF_RDD{
    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparksql: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境
        val spark: SparkSession = SparkSession.builder().config(sparksql).getOrCreate()

        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("Spark/input/user.json")

        val rdd: RDD[Row] = df.rdd
        val ageToOneRDD: RDD[(Long, Int)] = rdd.map(
            row => {
                (row.getLong(0), 1)
            }
        )
        val ageToCount: (Long, Int) = ageToOneRDD.reduce(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2)
            }
        )

        println("平均年龄 = " + ageToCount._1 / ageToCount._2)

        // TODO 关闭环境对象
        spark.close()
    }
    case class Person(name:String , age:Int)

}
