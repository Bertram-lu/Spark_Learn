package com.atguigu.spark.project.common

import com.atguigu.spark.project.util.ProjectUtil

trait TDao {

    def readFile( path:String ) = {
        ProjectUtil.sparkContext().textFile(path)
    }
}
