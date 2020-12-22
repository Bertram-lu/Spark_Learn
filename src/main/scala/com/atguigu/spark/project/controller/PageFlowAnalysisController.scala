package com.atguigu.spark.project.controller

import com.atguigu.spark.project.common.TController
import com.atguigu.spark.project.service.PageFlowAnalysisService

/**
  * 页面单跳转换率
  */
class PageFlowAnalysisController extends TController{
    private val pageFlowAnalysisService = new PageFlowAnalysisService
    override def execute(): Unit = {
        val result = pageFlowAnalysisService.analysis()
        //result.foreach(println)
    }
}
