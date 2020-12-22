package com.atguigu.spark.project.controller

import com.atguigu.spark.project.common.TController
import com.atguigu.spark.project.service.HotCategoryAnalysisTop10Service


/**
  * 热门品类Top10控制器对象
  */
class HotCategoryAnalysisTop10Controller extends TController{
    private val hotCategoryAnalysisTop10Service = new HotCategoryAnalysisTop10Service
    override def execute(): Unit = {
        //val result = hotCategoryAnalysisTop10Service.analysis()
        //val result = hotCategoryAnalysisTop10Service.analysis2()
        val result = hotCategoryAnalysisTop10Service.analysis4()
        result.foreach(println)
    }
}
