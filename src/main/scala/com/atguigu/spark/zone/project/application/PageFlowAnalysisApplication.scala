package com.atguigu.spark.zone.project.application

import com.atguigu.spark.zone.project.common.TApplication
import com.atguigu.spark.zone.project.controller.PageFlowAnalysisController

object PageFlowAnalysisApplication extends App with TApplication{

    start(appName = "PageFlowAnalysis") {
        val controller = new PageFlowAnalysisController
        controller.execute()
    }

}
