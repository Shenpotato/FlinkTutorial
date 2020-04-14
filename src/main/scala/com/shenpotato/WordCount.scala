package com.shenpotato

import org.apache.flink.api.scala._

// 批处理word count程序
object WordCount {
  def main(args: Array[String]): Unit = {
    // 创建一个执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    // 从文件中取数据
    val inputPath = "/Users/shenzhengtao/Code/IdeaWorkspace/FlinkTutorial/src/main/resources/hello.txt"
    val inputDataSet = env.readTextFile(inputPath);

    // 切分数据得到word，按word做分组聚合
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
      .map( (_,1) )
      .groupBy(0)
      .sum(1)

    wordCountDataSet.print()
  }
}
