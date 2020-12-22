package com.atguigu.spark.zone.streaming

object SparkStreaming14_DStream_Window {
    def main(args: Array[String]): Unit = {

        val list = List(1,2,3,4,5,6,7,8,9)

        // overflow : 滚动 -> StackOverflowError -> 栈溢出
        // 滑动
        // flatMap => 整体->个体
        // sliding => 整体连续部分（3） -> 整体
        // 将sliding中的范围称之为窗口，其中的数据就称之为窗口数据
        // 窗口可以动态调整，向后滑动。
        val iterator: Iterator[List[Int]] = list.sliding(3,2)
        while(iterator.hasNext) {
            println(iterator.next())
        }

    }

}
