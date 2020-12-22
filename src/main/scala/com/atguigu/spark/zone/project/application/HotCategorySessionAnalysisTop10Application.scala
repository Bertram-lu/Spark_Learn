package com.atguigu.spark.zone.project.application

import com.atguigu.spark.zone.project.common.TApplication
import com.atguigu.spark.zone.project.controller.{HotCategoryAnalysisTop10Controller, HotCategorySessionAnalysisTop10Controller}

object HotCategorySessionAnalysisTop10Application extends App with TApplication{

    start(appName = "HotCategoryAnalysisTop10") {

        val controller = new HotCategorySessionAnalysisTop10Controller

        controller.execute()
    }

}
