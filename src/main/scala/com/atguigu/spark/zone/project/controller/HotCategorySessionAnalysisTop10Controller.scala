package com.atguigu.spark.zone.project.controller

import com.atguigu.spark.zone.project.bean
import com.atguigu.spark.zone.project.common.TController
import com.atguigu.spark.zone.project.service.{HotCategoryAnalysisTop10Service, HotCategorySessionAnalysisTop10Service}

class HotCategorySessionAnalysisTop10Controller extends TController{

    private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service
    private val hotCategorySessionAnalysisTop10Service = new HotCategorySessionAnalysisTop10Service

    override def execute(): Unit = {
        val categories: List[bean.HotCategory] = hotCategoryAnalysisTop10Service.analysis5()
        val result = hotCategorySessionAnalysisTop10Service.analysis1(categories)
        result.foreach(println)
    }
}
