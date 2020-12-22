package com.atguigu.spark.zone.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL10_Load_Mysql {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

        // TODO 读取MySQL数据库
        import spark.implicits._
        val mysqlDF = spark.read.format("jdbc")
            .option("url" , "jdbc:mysql://hadoop102:3306/test")
            .option("driver" , "com.mysql.jdbc.Driver")
            .option("user" , "root")
            .option("password" , "123456")
            .option("dbtable" , "student")
            .load().show

        val prop = new Properties()
        prop.setProperty("user" , "root")
        prop.setProperty("password" , "123456")
        prop.setProperty("driver" , "com.mysql.jdbc.Driver")
        spark.read.jdbc("jdbc:mysql://hadoop102:3306/test" , "student" , prop)


        spark.close()
    }

}
