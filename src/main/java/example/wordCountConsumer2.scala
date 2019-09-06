package example

import `val`.KafkaProducerVal
import mould2.FastConsumer

object wordCountConsumer2 {
  def main(args: Array[String]): Unit = {
    FastConsumer("local[*]",
      "master:2181,slave:2181",
      "master:9092,,slave:9092",
      "groupName",10
    )
      .planbyWindow[(String,Int)]("wordCount")(
      ds=>ds.mapPartitions(_.map(a=>a.value().asInstanceOf[String]->1)).reduceByKey(_+_),"windowTest")(
      windowFun = windowValue=>println(windowValue),5,5
    )

      .planbyKafkaCache("wordCount")(
        DSplan = ds=>ds.mapPartitions(_.map(a=>a.value().asInstanceOf[String]->1)).reduceByKey(_+_).map(_.toString()),
        "cacheTest")(
        kafkaProducerConf = Map[String,Object](
          KafkaProducerVal.BOOTSTRAP_SERVERS->"master:9092,slave:9092",
          KafkaProducerVal.ACKS->"all",
          KafkaProducerVal.RETRIES->"0",
          KafkaProducerVal.BATCH_SIZE->"16384",
          KafkaProducerVal.LINGER_MS->"1",
          KafkaProducerVal.KEY_SERIALIZER->"org.apache.kafka.common.serialization.StringSerializer",
          KafkaProducerVal.VALUE_SERIALIZER->"org.apache.kafka.common.serialization.StringSerializer"
        ),"cacheTest",10)
      .foreach[String]("cacheTest")("cacheTest",a=>println("拿到缓存数据，准备执行业务"))
      .start
  }
}
