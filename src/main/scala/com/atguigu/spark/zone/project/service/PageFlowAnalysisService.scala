package com.atguigu.spark.zone.project.service

import com.atguigu.spark.zone.project.bean.UserVisitAction
import com.atguigu.spark.zone.project.common.TService
import com.atguigu.spark.zone.project.dao.PageFlowAnalysisDao
import org.apache.spark.rdd.RDD

class PageFlowAnalysisService extends TService{

    private val pageFlowAnalysisDao = new PageFlowAnalysisDao

    override def analysis() = {

        val fileRDD: RDD[String] = pageFlowAnalysisDao.readFile("Spark/input/user_visit_action.txt")

        val actionRDD: RDD[UserVisitAction] = fileRDD.map(
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

        val actionCacheRDD: RDD[UserVisitAction] = actionRDD.cache()

        val flowids = List(1,2,3,4,5,6,7)

        val zips: List[(Int, Int)] = flowids.zip(flowids.tail)

        val zipString: List[String] = zips.map {
            case (id1, id2) => {
                id1 + "-" + id2
            }
        }

        val filterRDD: RDD[UserVisitAction] = actionCacheRDD.filter(
            action => {
                flowids.contains(action.page_id.toInt)
            }
        )

        val pageToOneRDD: RDD[(Long, Int)] = filterRDD.map(
            action => {
                (action.page_id, 1)
            }
        )

        val pageToSumRDD: RDD[(Long, Int)] = pageToOneRDD.reduceByKey(_+_)

        val pageCount: Array[(Long, Int)] = pageToSumRDD.collect()
        val pageCountMap: Map[Long, Int] = pageCount.toMap


        val groupRDD: RDD[(String, Iterable[UserVisitAction])] = actionRDD.groupBy(_.session_id)

        val rdd: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                val sotAction: List[UserVisitAction] = iter.toList.sortWith(
                    (left, right) => {
                        left.action_time < right.action_time
                    }
                )

                val ids: List[Long] = sotAction.map(_.page_id)

                val zipIds: List[(Long, Long)] = ids.zip(ids.tail)

                val zipIdToOne: List[(String, Int)] = zipIds.map {
                    case (id1, id2) => {
                        (id1 + "-" + id2, 1)
                    }
                }

                zipIdToOne.filter(
                    t => {
                        zipString.contains(t._1)
                    }
                )
            }
        )
        val idToOneList: RDD[List[(String, Int)]] = rdd.map(_._2)
        val idToOneRDD: RDD[(String, Int)] = idToOneList.flatMap(list=>list)

        val idToSumRDD: RDD[(String, Int)] = idToOneRDD.reduceByKey(_+_)

        idToSumRDD.foreach{
            case (pageids , count) => {
                val ids: Array[String] = pageids.split("-")
                val count1: Int = pageCountMap(ids(0).toLong)
                println(pageids + "转换率为" + (count.toDouble / count1))
            }
        }
    }

}
