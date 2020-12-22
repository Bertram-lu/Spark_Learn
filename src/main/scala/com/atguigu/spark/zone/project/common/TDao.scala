package com.atguigu.spark.zone.project.common

import com.atguigu.spark.zone.project.util.ProjectUtil

trait TDao {

    def readFile ( path : String)  = {
        ProjectUtil.sparkContext().textFile(path)
    }

}
