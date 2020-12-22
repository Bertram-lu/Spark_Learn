package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL12_Hive {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
        import spark.implicits._

        spark.sql("show databases").show

        spark.close()
    }

}
