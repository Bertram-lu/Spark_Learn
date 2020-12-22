package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}

object SparkSQL16_Req_Impl2 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

        val cityRemarkUDAF = new CityRemarkUDAF

        spark.udf.register("cityRemark",cityRemarkUDAF)

        spark.sql("use sparksql")

        spark.sql(
            """
              |select
              |    a.*,
              |    p.product_name,
              |    c.area,
              |    c.city_name
              |from user_visit_action a
              |join product_info p on a.click_product_id = p.product_id
              |join city_info c on c.city_id = a.city_id
              |where a.click_product_id > -1
            """.stripMargin).createOrReplaceTempView("t1")
        // TODO 2. 将数据根据地区，商品名称分组。
        //         使用UDAF函数来聚合城市点击比率
        spark.sql(
            """
              |select
              |   area,
              |   product_name,
              |   count(*) as clickCount,
              |   cityRemark(city_name) as cityRemark
              |from t1 group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")
        // TODO 3. 统计商品点击次数总和, 取Top3
        spark.sql(
            """
              |select
              |    *,
              |    rank() over ( partition by area order by clickCount desc ) as rank
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")

        spark.sql(
            """
              |select
              |    *
              |from t3
              |where rank <= 3
            """.stripMargin).show

        spark.close
    }

    class CityRemarkUDAF extends UserDefinedAggregateFunction {
        override def inputSchema: StructType = {
            StructType(Array(
                StructField("citiName" , StringType)
            ))
        }

        override def bufferSchema: StructType = {
            StructType(Array(
                StructField("totalcount" , LongType),
                StructField("cityToCount" , MapType(StringType , LongType))
            ))
        }

        override def dataType: DataType = StringType

        override def deterministic: Boolean = true

        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            buffer(1) = Map[String,Long]()
        }

        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer(0) = buffer.getLong(0) + 1

            val map: Map[String, Long] = buffer.getAs[Map[String,Long]](1)
            val cityName: String = input.getString(0)
            val count: Long = map.getOrElse(cityName,0L)
            buffer(1) = map.updated(cityName,count + 1L)

            //buffer(1) = map
        }

        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)

            val map1: Map[String, Long] = buffer1.getAs[Map[String,Long]](1)
            val map2: Map[String, Long] = buffer2.getAs[Map[String,Long]](1)

            buffer1(1) = map1.foldLeft(map2) {
                case( map , (k,v)) => {
                    map.updated(k,map.getOrElse(k,0L) + v)
                    //map
                }
            }
        }

        override def evaluate(buffer: Row): Any = {
            val map: Map[String, Long] = buffer.getAs[Map[String,Long]](1)
            val totalcnt: Long = buffer.getLong(0)

            val cityToCountList: List[(String, Long)] = map.toList.sortWith(
                (left, right) => {
                    left._2 > right._2
                }
            ).take(2)

            val s = new StringBuilder()
            val m = new StringBuilder()
            var n = new Array[Long](1)
            cityToCountList.foreach {
                case (city , cnt) => {
                    s.append(city + " " + (cnt * 100 / totalcnt) + "% " )
                    n(0) = n(0) -  (cnt * 100 / totalcnt)
                }
            }
            val other: String = m.append("其他" + (100 + n(0)) + "%").toString()
            s.toString() + other
        }
    }

}
