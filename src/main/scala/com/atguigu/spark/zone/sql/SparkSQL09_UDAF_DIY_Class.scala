package com.atguigu.spark.zone.sql


import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}

object SparkSQL09_UDAF_DIY_Class {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

        val df: DataFrame = spark.read.json("Spark/input/user.json")

        val udaf = new MyAvgAgeUDAFClass

        val column: TypedColumn[User, Double] = udaf.toColumn

        import spark.implicits._

        val ds: Dataset[User] = df.as[User]

        ds.select(column).show

        spark.close()

    }
    case class User(username:String , userage:Long)

    case class AgeBuffer(totalage:Long , totalcnt:Long)

    class MyAvgAgeUDAFClass extends Aggregator[User,AgeBuffer,Double]{
        override def zero: AgeBuffer = {
            AgeBuffer(0L,0L)
        }

        override def reduce(b: AgeBuffer, a: User): AgeBuffer = {
            AgeBuffer(b.totalage + a.userage , b.totalcnt + 1L)
        }

        override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
            AgeBuffer(b1.totalage + b2.totalage , b1.totalcnt + b2.totalcnt)
        }

        override def finish(reduction: AgeBuffer): Double = {
            reduction.totalage.toDouble / reduction.totalcnt
        }

        override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product

        override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
    }


}
