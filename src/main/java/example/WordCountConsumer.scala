package example

import mould.{FastConsumer, Plan}

//注意：spark运行时的计划内的对象必须为闭包对象！
object WordCountConsumer {
  def main(args: Array[String]): Unit = {
    FastConsumer(
      "local[*]",
      "master:2181,slave:2181",
      "master:9092,,slave:9092",
      "wordCount"
    ).plan(Plan.wordCountPlan)
      .start
  }
}
