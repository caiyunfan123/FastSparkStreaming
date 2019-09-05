package example

import mould.{FastConsumer, Plan}

object Test {
  def main(args: Array[String]): Unit = {
    FastConsumer("local[*]","master:2181,slave:2181","master:9092,slave:9092","wordCount")
      .plan[String,Int](Plan(
      str=>(str,1),
      (v1,v2)=>v1+v2,
      (k,v)=>println(k,v)
    )).start
  }
}
