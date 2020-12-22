package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

object SparkSQL07_UDAF_Acc{
    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparksql: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境
        val spark: SparkSession = SparkSession.builder().config(sparksql).getOrCreate()

        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("Spark/input/user.json")

        val acc = new MyAvgAgeAcc

        spark.sparkContext.register(acc,"ageAvg")

        df.foreach(
            row => {
                acc.add(row.getLong(0))
            }
        )

        val (totalage,totalcnt) = acc.value

        println(totalage.toDouble / totalcnt)

        // TODO 关闭环境对象
        spark.close()
    }
    class MyAvgAgeAcc extends AccumulatorV2[Long , (Long , Long)]{

        var buffer = (0L,0L)

        override def isZero: Boolean = {
            buffer._1 == 0L && buffer._2 == 0L
        }

        override def copy(): AccumulatorV2[Long, (Long, Long)] = {
            new MyAvgAgeAcc
        }

        override def reset(): Unit = {
            buffer = (0L , 0L)
        }

        override def add(v: Long): Unit = {
            buffer = (buffer._1 + v , buffer._2 + 1)
        }

        override def merge(other: AccumulatorV2[Long, (Long, Long)]): Unit = {
            val (oage,ocnt) = other.value
            buffer = (buffer._1 + oage , buffer._2 + ocnt)
        }

        override def value: (Long, Long) = buffer
    }

}
