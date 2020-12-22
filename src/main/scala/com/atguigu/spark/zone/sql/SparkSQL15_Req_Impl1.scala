package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL15_Req_Impl1 {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

        spark.sql("use sparksql")

        spark.sql(
            """
              |select
              |	a.*,
              |	p.product_name,
              |	c.area,
              |	c.city_name
              |from user_visit_action a
              |join product_info p on a.click_product_id = p.product_id
              |join city_info c on c.city_id = a.city_id
              |where a.click_product_id > -1
            """.stripMargin).createOrReplaceGlobalTempView("t1")

        spark.sql(
            """
              |select
              |	area,
              |	product_name,
              |	count(*) as clickCount
              |from t1
            """.stripMargin).createOrReplaceGlobalTempView("t2")

        spark.sql(
            """
              |select
              |	*,
              |	rank() over ( partition by area order by clickCount desc ) as rank
              |from t2
            """.stripMargin).createOrReplaceGlobalTempView("t3")

        spark.sql(
            """
              |select
              | *
              |from t3
              |where rank < 3
            """.stripMargin).show

        spark.close()
    }

}
