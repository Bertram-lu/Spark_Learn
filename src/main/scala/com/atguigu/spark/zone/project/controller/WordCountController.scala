package com.atguigu.spark.zone.project.controller

import com.atguigu.spark.zone.project.common.TController
import com.atguigu.spark.zone.project.service.WordCountService

class WordCountController extends TController{

    private val wordCountService : WordCountService = new WordCountService

    def execute() = {

        val result = wordCountService.analysis()

        result.collect().foreach(println)
    }
}
