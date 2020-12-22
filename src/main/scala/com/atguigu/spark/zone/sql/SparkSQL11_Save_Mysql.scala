package com.atguigu.spark.zone.sql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQL11_Save_Mysql {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

        // TODO 保存MySQL数据库
        val mysqlDF = spark.read.format("jdbc")
            .option("url" , "jdbc:mysql://hadoop102:3306/test")
            .option("driver" , "com.mysql.jdbc.Driver")
            .option("user" , "root")
            .option("password" , "123456")
            .option("dbtable" , "student")
            .load()

        mysqlDF.write
                .format("jdbc")
            .option("url", "jdbc:mysql://hadoop102:3306/test")
            .option("user", "root")
            .option("password", "123456")
            .option("dbtable", "student1")
            //.mode("append")
            .mode(SaveMode.Append)
            .save()


        spark.close()
    }

}
