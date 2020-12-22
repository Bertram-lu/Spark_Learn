package com.atguigu.spark.zone.project.service

import com.atguigu.spark.zone.project.common.TService
import com.atguigu.spark.zone.project.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

    private val wordCountDao : WordCountDao = new WordCountDao

    override def analysis () ={

        val dataRDD = wordCountDao.readFile("Spark/input/1.txt")

        val wordRDD: RDD[String] = dataRDD.flatMap(_.split(" "))

        val word2OneRDD: RDD[(String, Int)] = wordRDD.map( (_,1) )

        val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_+_)

        word2CountRDD

    }
}
