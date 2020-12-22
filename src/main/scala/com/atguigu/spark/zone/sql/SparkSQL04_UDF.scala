package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL04_UDF {
    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparksql: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境
        val spark: SparkSession = SparkSession.builder().config(sparksql).getOrCreate()

        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("Spark/input/user.json")

        spark.udf.register("changeName" , (name:String)=>{
            "姓名 ：" + name
        })

        spark.udf.register("desc" , (age:Int) => {
            "年龄 ：" + age
        })

        // TODO 创建临时表
        df.createTempView("user")

        spark.sql("select changeName(username) , desc(userage) from user").show

        // TODO 关闭环境对象
        spark.close()
    }
    case class Person(name:String , age:Int)

}
