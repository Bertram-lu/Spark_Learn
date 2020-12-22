package com.atguigu.spark.zone.project.controller

import com.atguigu.spark.zone.project.common.TController
import com.atguigu.spark.zone.project.service.PageFlowAnalysisService

class PageFlowAnalysisController extends TController{

    private val pageFlowAnalysisService = new PageFlowAnalysisService

    override def execute(): Unit = {
        val result: Unit = pageFlowAnalysisService.analysis()
    }
}
