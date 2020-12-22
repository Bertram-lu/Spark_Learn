package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL01_DataFrame {
    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparksql: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境
        val spark: SparkSession = SparkSession.builder().config(sparksql).getOrCreate()

        // TODO DataFrame
        val df: DataFrame = spark.read.json("Spark/input/user.json")

        // TODO 使用SQL的语法访问数据
        //df.createTempView("user")
        //df.createGlobalTempView("guser")

        //spark.sql("select avg(userage) from user").show
        //spark.sql("select * from global_temp.guser").show
        //spark.newSession().sql("select * from global_temp.guser").show

        // TODO 使用DSL语法访问数据
        // 需要导入隐式转换规则:
        // 这里的spark不是包的名称，表示SparkSQL上下文环境对象变量的名称
        // TODO import 可以导入val变量内部的属性，方法。var是不可以的。
        import spark.implicits._
        df.select("username" , "userage").show
        df.select($"username" , $"userage").show
        df.select('userage).show


        // TODO 关闭环境对象
        spark.close()
    }

}
