package com.atguigu.spark.zone.project.application

import com.atguigu.spark.zone.project.common.TApplication
import com.atguigu.spark.zone.project.controller.HotCategoryAnalysisTop10Controller

object HotCategoryAnalysisTop10Application extends App with TApplication{

    start(appName = "HotCategoryAnalysisTop10") {

        val controller = new HotCategoryAnalysisTop10Controller

        controller.execute()
    }

}
