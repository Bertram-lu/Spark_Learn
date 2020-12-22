package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL02_DataSet {
    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparksql: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境
        val spark: SparkSession = SparkSession.builder().config(sparksql).getOrCreate()

        // TODO 导入隐式转换规则
        // TODO 无论是否用到隐式转换，都需要增加
        import spark.implicits._

        val list = List(
            Person("zhangsan", 30),
            Person("lisi", 40),
            Person("wangwu", 50)
        )
        val ds: Dataset[Person] = list.toDS

        // TODO 关闭环境对象
        spark.close()
    }
    case class Person(name:String , age:Int)

}
