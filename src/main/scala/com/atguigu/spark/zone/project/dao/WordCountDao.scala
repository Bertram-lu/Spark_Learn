package com.atguigu.spark.zone.project.dao

import com.atguigu.spark.zone.project.common.TDao
import com.atguigu.spark.zone.project.util.ProjectUtil
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao{

    def readDate():RDD[String] = {

        ProjectUtil.sparkContext().makeRDD(List("Hello" , "Scala"))
    }

}
