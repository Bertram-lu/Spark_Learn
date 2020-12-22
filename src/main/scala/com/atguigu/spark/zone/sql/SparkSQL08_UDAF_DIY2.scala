package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQL08_UDAF_DIY2 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")

        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

        val df: DataFrame = spark.read.json("Spark/input/user.json")

        val udaf = new MyAvgAgeUDAF

        spark.udf.register("avgAge" , udaf)

        df.createTempView("user")

        spark.sql("select avgAge(userage) from user").show

        spark.close()
    }
    class MyAvgAgeUDAF extends UserDefinedAggregateFunction{

        var buffer = (0L , 0L)

        override def inputSchema: StructType = {
            StructType(Array(
                StructField("age" , LongType)
            ))
        }

        override def bufferSchema: StructType = {
            StructType(Array(
                StructField("totalage" , LongType),
                StructField("totacnt" , LongType)
            ))
        }

        override def dataType: DataType = DoubleType

        override def deterministic: Boolean = true

        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = 0L
        }

        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer(0) = buffer.getLong(0) + input.getLong(0)
            buffer(1) = buffer.getLong(1) + 1L
        }

        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
            buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
        }

        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0).toDouble / buffer.getLong(1)
        }
    }
}
