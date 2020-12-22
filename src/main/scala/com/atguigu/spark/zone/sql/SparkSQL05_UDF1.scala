package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL05_UDF1{
    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparksql: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境
        val spark: SparkSession = SparkSession.builder().config(sparksql).getOrCreate()

        // TODO 读取JSON数据
        val df: DataFrame = spark.read.json("Spark/input/user.json")

        // TODO 读取数据时，DataFrame已经将结构处理好（顺序）
        // 默认的顺序是按照属性的字典顺序
        // userage, username,
        // select username, userage from user
        // 和JDBC处理方式很像
        import spark.implicits._

        val ds: Dataset[String] = df.map(
            row => {
                "Name:" + row.getString(1)
            }
        )

        ds.createTempView("user")

        spark.sql("select * from user").show

        // TODO 关闭环境对象
        spark.close()
    }
    case class Person(name:String , age:Int)

}
