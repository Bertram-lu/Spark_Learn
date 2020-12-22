package com.atguigu.spark.project.dao


import com.atguigu.spark.project.common.TDao
import com.atguigu.spark.project.util.ProjectUtil
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

class WordCountDao extends TDao{

    def readData() : RDD[String] = {
        ProjectUtil.sparkContext().makeRDD(List("Hello", "Scala"))
    }
}
