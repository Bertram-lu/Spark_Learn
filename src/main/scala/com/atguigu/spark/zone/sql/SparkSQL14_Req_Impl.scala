package com.atguigu.spark.zone.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSQL14_Req_Impl {
    def main(args: Array[String]): Unit = {

        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
        val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

        spark.sql("use sparksql")

        spark.sql(
            """
              |        select
              |            *
              |        from (
              |            select
              |                *,
              |                rank() over ( partition by area order by clickCount desc ) as rank
              |            from (
              |                select
              |                   area,
              |                   product_name,
              |                   count(*) as clickCount
              |                from (
              |
              |                    select
              |                        a.*,
              |                        p.product_name,
              |                        c.area,
              |                        c.city_name
              |                    from user_visit_action a
              |                    join product_info p on a.click_product_id = p.product_id
              |                    join city_info c on c.city_id = a.city_id
              |                    where a.click_product_id > -1
              |
              |                ) t1 group by area, product_name
              |            ) t2
              |        ) t3
              |        where rank <= 3
            """.stripMargin).show


        spark.close()
    }

}
