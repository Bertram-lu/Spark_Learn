package com.atguigu.spark.zone.project.common

import com.atguigu.spark.zone.project.util.ProjectUtil
import org.apache.hadoop.mapred.Master
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

    def start(master: String = "local[*]" , appName : String = "application")(op : => Unit) = {
        //创建Spark的配置对象
        ProjectUtil.sparkContext(master,appName)

        try {
            op
        } catch {
            case ex:Exception => ex.printStackTrace
        }


        ProjectUtil.stop()
    }
}
