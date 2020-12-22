package com.atguigu.spark.project.application

import com.atguigu.spark.project.common.TApplication
import com.atguigu.spark.project.controller.HotCategoryAnalysisTop10Controller


/**
  * 热门品类Top10应用
  */
object HotCategoryAnalysisTop10Application extends App with TApplication{

    start( appName = "HotCategoryAnalysisTop10" ) {
        val controller = new HotCategoryAnalysisTop10Controller
        controller.execute()
    }

}
