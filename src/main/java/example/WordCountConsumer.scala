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
    )
      .plan(Plan.wordCountPlan)
      .plan[String,(Int,Int)](
      Plan(
        (_,(1,2)),(a,b)=>(a._1+a._1,a._2+a._2),(a,b)=>println((a,b))
      ))
      .start
  }
}
