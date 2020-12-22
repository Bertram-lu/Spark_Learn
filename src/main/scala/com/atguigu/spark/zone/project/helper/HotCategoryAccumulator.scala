package com.atguigu.spark.zone.project.helper


import com.atguigu.spark.zone.project.bean.HotCategory
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String, HotCategory]]{

    private var hotCategoryMap = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = hotCategoryMap.isEmpty

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
        new HotCategoryAccumulator
    }

    override def reset(): Unit = {
        hotCategoryMap.clear()
    }

    override def add(v: (String, String)): Unit = {
        val categoryId = v._1
        val actionType = v._2

        val hc: HotCategory = hotCategoryMap.getOrElse(categoryId,HotCategory(categoryId,0,0,0))
        actionType match {
            case "click" => hc.clickCount += 1
            case "order" => hc.orderCount += 1
            case "pay" => hc.payCount += 1
            case _ =>
        }
        hotCategoryMap.update(categoryId,hc)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
        other.value.foreach{
            case (cid,hotCategory) => {
                val hc: HotCategory = hotCategoryMap.getOrElse(cid,HotCategory(cid,0,0,0))
                hc.clickCount += hotCategory.clickCount
                hc.orderCount += hotCategory.orderCount
                hc.payCount += hotCategory.payCount

                hotCategoryMap(cid) = hc
            }
        }
    }

    override def value: mutable.Map[String, HotCategory] = hotCategoryMap

    /*private val hotCategoryMap = mutable.Map[String,HotCategory]()

    override def isZero: Boolean = hotCategoryMap.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, HotCategory]] = {
        new HotCategoryAccumulator
    }

    override def reset(): Unit = {
        hotCategoryMap.clear()
    }

    override def add(v: String): Unit = {
        val datas: Array[String] = v.split("_")
        if (datas(6) != "-1") {
            val clickCount: Long = hotCategoryMap.getOrElse(datas(6) , HotCategory(datas(6),0,0,0)).clickCount
            hotCategoryMap.update(datas(6),HotCategory(datas(6),clickCount + 1,0,0))
        } else if (datas(8) != "null") {
            datas(8).split(",").foreach(
                id => {
                    val orderCount: Long = hotCategoryMap.getOrElse(id,HotCategory(id,0,0,0)).orderCount
                    hotCategoryMap.update(datas(8),HotCategory(datas(8),0,orderCount + 1 ,0))
                }
            )
        } else if (datas(10) != "null") {
            datas(10).split(",").foreach(
                id => {
                    val payCount: Long = hotCategoryMap.getOrElse(id,HotCategory(id,0,0,0)).payCount
                    hotCategoryMap.update(datas(10),HotCategory(datas(8),0,payCount + 1 ,0))
                }
            )
        }
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, HotCategory]]): Unit = {
        other.value.foreach {
            case(id,category) => {
                val category1 = hotCategoryMap.getOrElseUpdate(id,HotCategory(id,0,0,0))
                category1.clickCount = category.clickCount + category1.clickCount
                category1.orderCount = category.orderCount + category1.orderCount
                category1.payCount = category.payCount + category1.payCount
            }
        }
    }

    override def value: mutable.Map[String, HotCategory] = hotCategoryMap*/

}
