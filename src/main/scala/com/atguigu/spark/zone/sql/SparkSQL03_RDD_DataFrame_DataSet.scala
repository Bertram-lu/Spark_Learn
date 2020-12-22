package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSQL03_RDD_DataFrame_DataSet {
    def main(args: Array[String]): Unit = {

        // TODO 创建配置信息
        val sparksql: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        // TODO 创建上下文环境
        val spark: SparkSession = SparkSession.builder().config(sparksql).getOrCreate()

        // TODO 导入隐式转换规则
        // TODO 无论是否用到隐式转换，都需要增加
        import spark.implicits._

        val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(
            ("zhangsan", 30), ("lisi", 40), ("wangwu", 50)
        ))

        // TODO RDD => DataFrame
        //val df: DataFrame = rdd.toDF("name","age")

        // TODO DataFrame => DataSet
        //val ds: Dataset[Person] = df.as[Person]
        //ds.show()

        // TODO RDD => DataSet
        val personRDD: RDD[Person] = rdd.map {
            case (name, age) => {
                Person(name, age)
            }
        }
        val ds: Dataset[Person] = personRDD.toDS()

        // TODO DataSet => DataFrame
        val df: DataFrame = ds.toDF()
        df.foreach(
            row => {
                println(row(0))
            }
        )

        // TODO 关闭环境对象
        spark.close()
    }
    case class Person(name:String , age:Int)

}
