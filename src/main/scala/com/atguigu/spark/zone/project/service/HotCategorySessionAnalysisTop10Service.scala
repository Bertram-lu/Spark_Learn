package com.atguigu.spark.zone.project.service

import com.atguigu.spark.zone.project.bean.{HotCategory, UserVisitAction}
import com.atguigu.spark.zone.project.common.TService
import com.atguigu.spark.zone.project.dao.HotCategorySessionAnalysisTop10Dao
import com.atguigu.spark.zone.project.util.ProjectUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD


class HotCategorySessionAnalysisTop10Service extends TService{

    private val hotCategorySessionAnalysisTop10Dao = new HotCategorySessionAnalysisTop10Dao

    def analysis1(categories:List[HotCategory]) = {

        // TODO 读取原始数据, 封装成样例类对象
        val fileRDD: RDD[String] = hotCategorySessionAnalysisTop10Dao.readFile("Spark/input/user_visit_action.txt")

        val actionRDD =
            fileRDD.map(
                line => {
                    val datas: Array[String] = line.split("_")
                    UserVisitAction(
                        datas(0),
                        datas(1).toLong,
                        datas(2),
                        datas(3).toLong,
                        datas(4),
                        datas(5),
                        datas(6).toLong,
                        datas(7).toLong,
                        datas(8),
                        datas(9),
                        datas(10),
                        datas(11),
                        datas(12).toLong
                    )
                }
            )

        val cids: List[String] = categories.map(_.id)
        val brocastIds: Broadcast[List[String]] = ProjectUtil.sparkContext().broadcast(cids)

        val filterRDD: RDD[UserVisitAction] = actionRDD.filter(
            action => {
                if (action.click_category_id != "-1") {
                    brocastIds.value.contains(action.click_category_id.toString)
                } else {
                    false
                }
            }
        )

        val categoryToSessionRDD: RDD[(String, Int)] = filterRDD.map(
            action => {
                (action.click_category_id + "_" + action.session_id, 1)
            }
        )

        val categoryToSessionSumRDD: RDD[(String, Int)] = categoryToSessionRDD.reduceByKey(_+_)

        val categoryToSessionSumRDD1: RDD[(String, (String, Int))] = categoryToSessionSumRDD.map {
            case (k, sum) => {
                val ks: Array[String] = k.split("_")
                (ks(0), (ks(1), sum))
            }
        }
        val groupRDD: RDD[(String, Iterable[(String, Int)])] = categoryToSessionSumRDD1.groupByKey()

        val mapValuesRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                iter.toList.sortWith(
                    (left, right) => {
                        left._2 > right._2
                    }
                ).take(10)
            }
        )
        mapValuesRDD.collect()
    }
}
