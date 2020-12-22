package com.atguigu.spark.zone.project.application

import com.atguigu.spark.zone.project.common.TApplication
import com.atguigu.spark.zone.project.controller.WordCountController

object WordCountApplivation extends App with TApplication{
    start (appName = "WordCount"){

        val controller = new WordCountController

        controller.execute()
    }
}
