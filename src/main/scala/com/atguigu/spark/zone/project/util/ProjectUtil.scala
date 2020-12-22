package com.atguigu.spark.zone.project.util

import org.apache.spark.{SparkConf, SparkContext}

object ProjectUtil {

    private val scLocal : ThreadLocal[SparkContext] = new ThreadLocal[SparkContext]

    def sparkContext(master: String = "local[*]" , appName : String = "application") = {

        var sc: SparkContext = scLocal.get()
        if (sc == null) {
            val conf = new SparkConf().setAppName(appName).setMaster(master)
            sc = new SparkContext(conf)
            scLocal.set(sc)
        }

        sc
    }

    def stop () = {
        val sc: SparkContext = scLocal.get()
        if (sc != null) {
            sc.stop()
        }
        scLocal.remove()
    }
}
