package com.atguigu.spark.project.application

import com.atguigu.spark.project.common.TApplication
import com.atguigu.spark.project.controller.PageFlowAnalysisController

/**
  * 页面单跳转换率
  */
object PageFlowAnalysisApplication extends App with TApplication{

    start( appName = "PageFlowAnalysis" ) {
        val controller = new PageFlowAnalysisController
        controller.execute()
    }

}
