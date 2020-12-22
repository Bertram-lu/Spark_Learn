package com.atguigu.spark.zone.project.controller

import com.atguigu.spark.zone.project.bean
import com.atguigu.spark.zone.project.common.TController
import com.atguigu.spark.zone.project.service.HotCategoryAnalysisTop10Service

class HotCategoryAnalysisTop10Controller extends TController{

    private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service

    override def execute(): Unit = {
        //val result = hotCategoryAnalysisTop10Service.analysis()
        //val result: Array[(String, (Int, Int, Int))] = hotCategoryAnalysisTop10Service.analysis2()
        //val result: Array[bean.HotCategory] = hotCategoryAnalysisTop10Service.analysis3()
        //val result: Array[bean.HotCategory] = hotCategoryAnalysisTop10Service.analysis4()
        val result: List[bean.HotCategory] = hotCategoryAnalysisTop10Service.analysis5()
        result.foreach(println)
    }
}
